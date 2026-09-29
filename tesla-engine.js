/* Tesla-engine.js - Drive-proof live TV playback for the livetv page.
 *
 * WHY THIS EXISTS
 * ---------------
 * The normal player on this page is video.js + application/x-mpegURL, which
 * means MSE feeding a <video> element. When the car is in Drive, that is the
 * thing that freezes: the picture locks while the audio keeps going. MSE and the
 * media element are the problem, not the stream.
 *
 * So this player never creates a media element for the video path. It fetches
 * the MPEG-TS segments itself and decodes them with WebCodecs, painting each
 * decoded frame onto a <canvas>:
 *
 *   .ts -> mux.js mp4.Transmuxer -> fMP4 -> mp4box.js -> elementary samples
 *       -> WebCodecs VideoDecoder -> canvas
 *       -> WebCodecs AudioDecoder -> ring buffer in an AudioWorklet -> speakers
 *
 * That is the same pipeline proven on the Sky Sports F1 page, generalised for
 * many channels and made restartable.
 *
 * WHAT IS DIFFERENT FROM THE F1 PAGE, AND WHY
 * -------------------------------------------
 * 1. open()/close() instead of a one-shot start(). A channel grid switches
 *    streams constantly, so every decoder, timer and audio node has to be
 *    torn down or they pile up: each switch would leak a VideoDecoder and a
 *    second audio graph, and the page would get slower the more you flick
 *    through channels.
 * 2. Master playlists are followed (#EXT-X-STREAM-INF). F1TV serves a master
 *    playlist whose variant is chosen by bandwidth, and the master has no
 *    segments of its own - reading it as a media playlist yields nothing.
 * 3. Non-media tracks are ignored. F1TV's TS carries scte_35 and timed_id3
 *    alongside the real audio; taking "the first audio track" would decode
 *    SCTE-35 ad markers as if they were sound.
 * 4. Queue caps scale with the real frame rate. Channels run at 25, 30 and
 *    59.94fps, and a cap sized for 25fps overflows on a 60fps one, dropping
 *    exactly the frames that were about to be shown.
 * 5. The audio sample rate comes from the stream, not a hardcoded 48000.
 *    nasa.4k.us is 32kHz; everything else is 48kHz. Forcing 48kHz on a 32kHz
 *    track makes the decoder resample or fail, and the ring then runs at the
 *    wrong rate against the device clock.
 * 6. Canvas size is capped. Decoding happens at source resolution, but the
 *    canvas is scaled down: a 1080p canvas at 60fps is pure paint cost for no
 *    visible gain in a car.
 */
(function (global) {
  'use strict';

  // How much already-downloaded media to keep ahead of playback. This is the
  // cushion that absorbs a slow segment fetch or a decode hiccup without the
  // listener hearing a gap.
  var BUFFER_TARGET_S = 12;

  var has = function (k) { return typeof global[k] !== 'undefined'; };

  function missingLibs(needAudio) {
    var miss = [];
    if (!has('muxjs') || !global.muxjs.mp4 || !global.muxjs.mp4.Transmuxer) miss.push('mux.js');
    if (!has('MP4Box')) miss.push('mp4box.js');
    if (!has('VideoDecoder') || !has('EncodedVideoChunk')) miss.push('WebCodecs VideoDecoder');
    if (needAudio && (!has('AudioDecoder') || !has('AudioData'))) miss.push('WebCodecs AudioDecoder');
    return miss;
  }

  // ==========================================================================
  // The ring buffer, as an AudioWorklet.
  //
  // The obvious implementation - one AudioBuffer plus a BufferSourceNode per
  // decoded audio frame - sounds bad. At 1024 samples a frame that is ~47
  // buffer sources per second, each scheduled independently, and any sub-block
  // scheduling error becomes a discontinuity: a click on every boundary. It
  // also allocates a buffer per frame.
  //
  // Instead the decoder output is poured into a ring buffer that the worklet
  // pulls at exactly one steady rate. One uninterrupted stream, no per-block
  // seams, and the decoder can run ahead into the buffer while playback stays
  // smooth.
  // ==========================================================================
  var RING_WORKLET = [
    'class LoFiRing extends AudioWorkletProcessor {',
    '  constructor(opts) {',
    '    super();',
    '    const p = (opts && opts.processorOptions) || {};',
    '    this.cap = p.capacity || sampleRate * 8;',
    '    this.ch = [new Float32Array(this.cap), new Float32Array(this.cap)];',
    '    this.w = 0; this.r = 0; this.avail = 0; this.pos = 0;',
    '    this.underruns = 0; this.fed = false; this.tick = 0;',
    // The stream's rate is not always the device's. nasa.4k.us is 32kHz and
    // the AudioContext is 48kHz, so reading one source sample per output sample
    // drains the ring 1.5x too fast and it starves continuously - a permanent
    // crackle. Linear interpolation between the source rate and the device rate
    // keeps the two clocks in step.
    '    this.srcRate = p.sourceRate || sampleRate;',
    '    this.step = this.srcRate / sampleRate;',
    '    this.port.onmessage = (e) => {',
    '      const d = e.data;',
    // Switching channels must not play out the tail of the previous one, so the
    // ring is emptied on a switch. fed resets too, so the underrun counter
    // measures the new channel rather than inheriting the old one's.
    '      if (d && d.flush) { this.w = 0; this.r = 0; this.avail = 0; this.pos = 0; this.fed = false; return; }',
    // A new channel may arrive at a different rate. Re-step and empty, because
    // the existing contents are in the old rate's timeline.
    '      if (d && d.rate) {',
    '        this.srcRate = d.rate; this.step = d.rate / sampleRate;',
    '        this.w = 0; this.r = 0; this.avail = 0; this.pos = 0; this.fed = false;',
    '        return;',
    '      }',
    '      const frames = d;',
    '      if (!frames || !frames.length) return;',
    '      const n = frames[0].length;',
    '      if (this.avail + n > this.cap) {',
    '        const room = Math.max(0, this.cap - this.avail);',
    '        for (let i = 0; i < room; i++) {',
    '          this.ch[0][this.w] = frames[0][i]; this.ch[1][this.w] = frames[1][i];',
    '          this.w = (this.w + 1) % this.cap;',
    '        }',
    '        this.avail += room;',
    '        return;',
    '      }',
    '      for (let i = 0; i < n; i++) {',
    '        this.ch[0][this.w] = frames[0][i]; this.ch[1][this.w] = frames[1][i];',
    '        this.w = (this.w + 1) % this.cap;',
    '      }',
    '      this.avail += n; this.fed = true;',
    '    };',
    '  }',
    '  process(inputs, outputs) {',
    '    const out = outputs[0];',
    '    if (!out || !out.length) return true;',
    '    const n = out[0].length;',
    '    let starved = false;',
    '    const L = this.ch[0], R = this.ch[1];',
    // One read per OUTPUT SAMPLE POSITION, not per channel. Advancing the read
    // pointer inside the per-channel loop consumed 2 samples per output frame on
    // a stereo stream, so the ring drained at exactly double rate, always ran
    // dry, and gave the right channel the wrong samples.
    '    for (let i = 0; i < n; i++) {',
    '      let l = 0, r = 0;',
    '      if (this.avail >= 1) {',
    '        const i0 = this.pos | 0;',
    '        const i1 = i0 + 1;',
    '        const f = this.pos - i0;',
    '        const a0 = i0 % this.cap, a1 = i1 % this.cap;',
    '        if (f > 0) {',
    '          l = L[a0] + (L[a1] - L[a0]) * f;',
    '          r = R[a0] + (R[a1] - R[a0]) * f;',
    '        } else { l = L[a0]; r = R[a0]; }',
    '        this.pos += this.step;',
    '        this.avail -= this.step;',
    '      } else { starved = true; }',
    '      out[0][i] = l;',
    '      if (out.length > 1) out[1][i] = r;',
    '      if (out.length > 2) for (let c = 2; c < out.length; c++) out[c][i] = l;',
    '      this.sum = (this.sum || 0) + l * l;',
    '      this.count = (this.count || 0) + 1;',
    '      if (this.count >= 2048) { this.rms = Math.sqrt(this.sum / this.count); this.count = 0; this.sum = 0; }',
    '    }',
    // Report sparingly. process() runs every 128 samples, ~375 times a second,
    // and posting on every call floods the main thread, which starves the paint
    // timer and the decoder feed. A tick every ~10 quanta is plenty.
    '    this.tick++;',
    // Only count a dropout once audio has actually been supplied. The graph is
    // built at tap time, well before the first segment is demuxed, so the ring
    // is legitimately empty for the first seconds - that is silence because
    // there is nothing yet, not a gap in playback.
    '    if (starved && this.fed) {',
    '      this.underruns++;',
    '      if (this.tick > 8) { this.tick = 0; this.port.postMessage({ starved: 1, underruns: this.underruns }); }',
    '    } else if (this.tick >= 8) {',
    '      this.tick = 0;',
    '      this.port.postMessage({ depth: this.avail / this.srcRate, rms: this.rms || 0 });',
    '    }',
    '    return true;',
    '  }',
    '}',
    "registerProcessor('lofi-ring', LoFiRing);"
  ].join('\n');

  function hex2(n) { return (n == null ? 0 : n).toString(16).padStart(2, '0'); }

  // avcC record -> the flat AVCDecoderConfigurationRecord WebCodecs wants.
  function avcCDescription(avcC) {
    if (!avcC || !avcC.SPS || !avcC.SPS.length || !avcC.PPS || !avcC.PPS.length) return undefined;
    var parts = [avcC.configurationVersion, avcC.AVCProfileIndication,
      avcC.profile_compatibility, avcC.AVCLevelIndication,
      0xFC | (avcC.lengthSizeMinusOne & 3), 0xE0 | avcC.SPS.length];
    for (var i = 0; i < avcC.SPS.length; i++) {
      var s = avcC.SPS[i].nalu;
      parts.push((s.length >> 8) & 255, s.length & 255);
      for (var j = 0; j < s.length; j++) parts.push(s[j]);
    }
    parts.push(avcC.PPS.length);
    for (var k = 0; k < avcC.PPS.length; k++) {
      var p = avcC.PPS[k].nalu;
      parts.push((p.length >> 8) & 255, p.length & 255);
      for (var m = 0; m < p.length; m++) parts.push(p[m]);
    }
    return new Uint8Array(parts);
  }

  // esds -> the AudioSpecificConfig WebCodecs wants for AAC.
  function ascFromEsds(esds) {
    try {
      var d = esds.esd.esdDescs[0].descs[0].descs;
      var i = 0;
      while (i < d.length) {
        if (d[i].tag === 0x05) return new Uint8Array(d[i].data);
        i++;
      }
    } catch (e) { /* fall through: the codec string alone is enough for most AAC */ }
    return undefined;
  }

  // ========================================================================
  // The player
  // ========================================================================
  function TeslaPlayer(opts) {
    this.opts = opts || {};
    this.canvas = this.opts.canvas;
    this.ctx2d = this.canvas.getContext('2d', { alpha: false });
    // Cap the canvas. Decoding is at source resolution either way, but a 1080p
    // canvas repainted at 60fps is a lot of paint for a car display.
    this.maxW = this.opts.maxWidth || 1280;
    this.gainValue = this.opts.gain || 4;
    this.gen = 0;          // bumped by close(); every async callback checks it
    this.live = null;      // the running stream, or null
    this.actx = null;      // AudioContext, kept across channels
    this.activated = false; // has a user gesture happened yet
    // The audio output chain, built once and shared by every channel.
    this.audio = { ready: false, pending: null, node: null, push: null, flush: null,
                   setRate: null, rate: 0, sourceRate: 0,
                   path: '', modErr: '', depth: 0, rms: 0, underruns: 0,
                   gain: null, limiter: null, analyser: null, buf: null };
    this.listeners = {};
  }

  TeslaPlayer.prototype.on = function (ev, fn) {
    (this.listeners[ev] = this.listeners[ev] || []).push(fn);
    return this;
  };
  TeslaPlayer.prototype.emit = function (ev, data) {
    var l = this.listeners[ev];
    if (!l) return;
    for (var i = 0; i < l.length; i++) { try { l[i](data); } catch (e) {} }
  };

  // ---- audio unlock -------------------------------------------------------
  // Must happen inside a real user gesture. Creating the AudioContext later,
  // when the first segment finishes demuxing, is a network callback and not a
  // gesture: strict autoplay policies then leave the context suspended, every
  // sample is discarded, and you get a perfect picture with total silence and
  // no error anywhere.
  TeslaPlayer.prototype.unlock = function () {
    var self = this;
    var AC = global.AudioContext || global.webkitAudioContext;
    if (!AC) return Promise.resolve(false);
    this.activated = true;
    if (!this.actx) {
      try { this.actx = new AC(); } catch (e) { return Promise.resolve(false); }
    }
    var p = this.actx.state === 'suspended' ? this.actx.resume() : Promise.resolve();
    return p.then(function () {
      // Some engines only fully unlock on a real source.
      try {
        var b = self.actx.createBuffer(1, 1, 44100);
        var s = self.actx.createBufferSource();
        s.buffer = b; s.connect(self.actx.destination); s.start(0);
      } catch (e) {}
      // Build the ring while the gesture is still valid.
      return self.ensureAudio();
    }).catch(function () { return false; });
  };

  // A 1kHz tone through the same output chain. If this is silent, audio is not
  // reaching the speakers and the problem is the environment, not the decode
  // path - otherwise the two are indistinguishable.
  TeslaPlayer.prototype.tone = function () {
    if (!this.actx) { this.emit('note', 'no audio context'); return; }
    if (this.actx.state === 'suspended') this.actx.resume().catch(function () {});
    var t = this.actx.currentTime;
    var o = this.actx.createOscillator();
    var g = this.actx.createGain();
    o.frequency.value = 1000;
    g.gain.setValueAtTime(0.0001, t);
    g.gain.exponentialRampToValueAtTime(0.25, t + 0.02);
    g.gain.setValueAtTime(0.25, t + 0.4);
    g.gain.exponentialRampToValueAtTime(0.0001, t + 0.5);
    o.connect(g); g.connect(this.actx.destination);
    o.start(t); o.stop(t + 0.55);
    this.emit('note', 'tone played');
  };

  // ---- lifecycle ----------------------------------------------------------
  TeslaPlayer.prototype.open = function (url, meta) {
    var self = this;
    this.close();                     // never two pipelines at once
    this.gen++;
    var gen = this.gen;

    var s = {
      url: url, meta: meta || {}, t0: Date.now(),
      base: url, segs: [], cursor: 0, lastUrl: null,
      target: 4, edgeLen: 0,
      tm: null, mbox: null, filePos: 0, initFed: false,
      vTrack: null, aTrack: null,
      vdec: null, adec: null,
      fq: [], pend: [], aPend: [],
      vT0: -1, mediaEnd: 0, frameIv: 0, lastFrameDue: -1,
      paintTimer: null, pumpTimer: null, rafId: 0, statsTimer: 0,
      stopped: false, fatal: null, firstFrameAt: 0,
      // Fixed, and deliberately generous. These hold the WORST-CASE buffer -
      // downloading happens a whole segment at a time, so the buffer oscillates
      // between the 12s target and target+segment (~25s) - for the heaviest
      // channel here (59.94fps, 13s segments): 2000 video samples is 33s at
      // 60fps, and 2400 AAC frames is 36s at 48kHz.
      //
      // They are NOT derived from the measured frame rate. An earlier version
      // sized them from a running average of the frame intervals, and one
      // outlier timestamp was enough to shrink the cap below a single segment's
      // worth of samples - at which point the queue overflowed on every segment
      // and dropped the oldest entries, which are the frames about to be shown.
      // A fixed cap cannot have that failure mode.
      caps: { fq: 40, v: 2000, a: 2400 },
      ivWin: [],
      aOutRms: 0,
      // counters
      drawn: 0, drops: 0, segsFetched: 0, samples: 0, frames: 0,
      aFrames: 0, aSeconds: 0, aUnderruns: 0, aQueue: 0, aRms: 0,
      codec: '-', width: 0, height: 0, srcRate: 0,
      decFps: 0, paintFps: 0, lastDecT: 0, lastPaintT: 0,
      gaps: [], lastGapAt: 0, buffered: 0
    };
    this.live = s;
    this.emit('state', this.state());

    var miss = missingLibs(true);
    if (miss.length) { this.fail('needs ' + miss.join(', ')); return Promise.resolve(false); }

    this.unlock().then(function () { return self.load(s, gen); });
    return Promise.resolve(true);
  };

  TeslaPlayer.prototype.load = function (s, gen) {
    var self = this;
    return this.loadPlaylist(s, gen).then(function () {
      if (s.stopped || self.gen !== gen) return;
      // Join several segments back from the edge. Combined with buffer-depth
      // pacing this lets the pipeline build a cushion before playback has to
      // rely on it; starting at the very edge leaves nothing to absorb a hiccup.
      s.cursor = Math.max(0, s.segs.length - 6);
      self.startMux(s, gen);
    }).catch(function (e) { self.fail('playlist: ' + e.message); });
  };

  // Fetch the playlist, following a master playlist to its best variant.
  TeslaPlayer.prototype.loadPlaylist = function (s, gen) {
    var self = this;
    return fetch(s.url, { cache: 'no-store' }).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      // The starlite URL 301s to a per-session CDN host; segment paths are
      // relative to THAT, not to the URL we asked for. Resolving against the
      // original yields "Resource not found" for every segment.
      var finalUrl = r.url || s.url;
      return r.text().then(function (t) { return { text: t, finalUrl: finalUrl }; });
    }).then(function (o) {
      if (s.stopped || self.gen !== gen) return null;
      var text = o.text, base = o.finalUrl;
      // A master playlist lists variants with #EXT-X-STREAM-INF and has no
      // segments of its own. F1TV is one. Pick the highest bandwidth variant:
      // the car can take it, and picking by bandwidth is more predictable than
      // guessing from a RESOLUTION attribute.
      var variant = pickVariant(text, base);
      if (variant) {
        s.base = variant;
        s.master = true;
        return fetch(variant, { cache: 'no-store' }).then(function (r2) {
          if (!r2.ok) throw new Error('variant HTTP ' + r2.status);
          var f2 = r2.url || variant;
          return r2.text().then(function (t2) { return { text: t2, finalUrl: f2 }; });
        });
      }
      return { text: text, finalUrl: base };
    }).then(function (o) {
      if (s.stopped || self.gen !== gen || !o) return;
      s.base = o.finalUrl;
      var p = parsePlaylist(o.text);
      if (!p.segs.length) throw new Error('no segments in playlist');
      s.target = p.target;
      s.segs = p.segs;
      s.edgeLen = p.segs.reduce(function (a, x) { return a + x.dur; }, 0);
      s.segDur = p.segs.length ? p.segs[0].dur : 4;
    });
  };

  // Highest-bandwidth variant, or null when this is already a media playlist.
  function pickVariant(text, base) {
    var lines = text.split(/\r?\n/);
    var best = null, bestBw = -1, pending = null;
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (line.indexOf('#EXT-X-STREAM-INF:') === 0) {
        pending = line;
      } else if (line && line.charAt(0) !== '#' && pending) {
        var bw = /BANDWIDTH=(\d+)/.exec(pending);
        var w = bw ? parseInt(bw[1], 10) : 0;
        if (w > bestBw) { bestBw = w; best = line; }
        pending = null;
      } else if (line && line.charAt(0) !== '#') {
        // A segment with no preceding STREAM-INF: this is a media playlist.
        return null;
      }
    }
    return best ? absolute(best, base) : null;
  }

  function absolute(u, base) {
    try { return new URL(u, base).href; } catch (e) { return u; }
  }

  function parsePlaylist(text) {
    var segs = [], dur = 0, target = 4, map = null;
    var lines = text.split(/\r?\n/);
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (!line) continue;
      if (line.indexOf('#EXT-X-TARGETDURATION:') === 0) {
        target = parseFloat(line.slice(21)) || target;
      } else if (line.indexOf('#EXT-X-MAP:') === 0) {
        var mu = /URI="([^"]+)"/.exec(line);
        if (mu) map = mu[1];
      } else if (line.indexOf('#EXTINF:') === 0) {
        dur = parseFloat(line.slice(8));
      } else if (line.charAt(0) !== '#') {
        segs.push({ url: line, dur: dur || target });
        dur = 0;
      }
    }
    return { segs: segs, target: target, map: map };
  }

  // ---- mux setup ----------------------------------------------------------
  TeslaPlayer.prototype.startMux = function (s, gen) {
    var self = this;
    s.mbox = MP4Box.createFile();
    s.tm = new muxjs.mp4.Transmuxer({ keepOriginalTimestamps: false, removeEPK: true });

    // mux.js 6 is an EventEmitter. It fires at a SEGMENT BOUNDARY, so segment
    // N's fMP4 only appears once N+1 has been pushed - which is why a single
    // push() produces nothing.
    s.tm.on('data', function (event) {
      if (s.stopped || self.gen !== gen) return;
      // The init segment (ftyp+moov) is fed exactly once. mux.js repeats it on
      // every event, and handing mp4box a second moov makes it rebuild its
      // sample tables against truns it has not seen; appendBuffer then throws
      // and the whole pipeline stops.
      if (event.initSegment && !s.initFed) { s.initFed = true; self.feedMp4(s, new Uint8Array(event.initSegment)); }
      if (event.data) self.feedMp4(s, new Uint8Array(event.data));
    });

    s.vdec = new VideoDecoder({
      output: function (frame) { self.onVideoFrame(s, frame); },
      error: function (e) { self.fail('video decoder: ' + e.message); }
    });

    s.pumpTimer = setInterval(function () {
      if (s.stopped || self.gen !== gen) return;
      self.feedDecoders(s, Date.now());
    }, 10);
    // The pump is a self-rescheduling loop rather than a tight while, so closing
    // a channel mid-fetch cannot leave an orphaned loop running.
    self.pump(s, gen);
    self.schedulePaint(s, gen);

    s.statsTimer = setInterval(function () {
      if (s.stopped || self.gen !== gen) return;
      self.emit('state', self.state());
    }, 500);
  };

  TeslaPlayer.prototype.feedMp4 = function (s, bytes) {
    if (!s.mbox || s.stopped) return;
    try {
      var ab = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
      ab.fileStart = s.filePos;
      s.mbox.appendBuffer(ab);
      s.filePos += ab.byteLength;
      if (!s.vTrack) this.setupTracks(s);
    } catch (e) { this.fail('demux: ' + e.message); }
  };

  // Pick the real video and audio tracks. getInfo() is only a summary; the
  // sample-entry box tree (where avcC and esds live) comes from getTrackById().
  TeslaPlayer.prototype.setupTracks = function (s) {
    var self = this;
    var info;
    try { info = s.mbox.getInfo(); } catch (e) { return; }
    if (!info) return;

    var tracks = (info.videoTracks || []);
    if (!tracks.length) { s.noVideo = (s.noVideo || 0) + 1; return; }
    var vt = tracks[0];
    s.vTrack = vt;

    // Choose the audio track by codec, not by position. F1TV's TS carries
    // scte_35 and timed_id3 beside the real audio; taking the first entry
    // decodes SCTE-35 ad markers as if they were sound.
    var at = null;
    var ats = info.audioTracks || [];
    for (var i = 0; i < ats.length; i++) {
      var c = (ats[i].codec || '') + ' ' + (ats[i].type || '');
      if (/mp4a|aac/i.test(c)) { at = ats[i]; break; }
      if (!at) at = ats[i];
    }
    s.aTrack = at || null;

    var cfg = this.videoConfig(s.mbox.getTrackById(vt.id));
    s.codec = cfg.codec;
    try {
      s.vdec.configure(cfg);
    } catch (e) { this.fail('video config: ' + e.message); return; }

    if (s.aTrack) { try { this.setupAudio(s, s.mbox.getTrackById(s.aTrack.id)); } catch (e) {} }

    s.mbox.setExtractionOptions(vt.id, null, { nbSamples: 90 });
    if (s.aTrack) s.mbox.setExtractionOptions(s.aTrack.id, null, { nbSamples: 200 });
    s.mbox.onSamples = function (id, user, samples) { self.onSamples(s, id, samples); };
    s.mbox.start();   // without start() mp4box never releases samples
  };

  TeslaPlayer.prototype.videoConfig = function (trak) {
    var entry = trak.mdia.minf.stbl.stsd.entries[0];
    var desc = avcCDescription(entry && entry.avcC);
    var codec = trak.codec || (entry && entry.avcC
      ? 'avc1.' + hex2(entry.avcC.AVCProfileIndication) + hex2(entry.avcC.profile_compatibility) + hex2(entry.avcC.AVCLevelIndication)
      : 'avc1.64001f');
    var cfg = { codec: codec, optimizeForLatency: true, hardwareAcceleration: 'prefer-hardware' };
    if (desc) cfg.description = desc;
    return cfg;
  };

  // ---- audio --------------------------------------------------------------
  // The audio graph is owned by the PLAYER, not by a stream. It is built once
  // and shared across every channel, and only the decoder feeding it changes.
  //
  // Building it per channel was a real bug: each switch created another
  // AudioWorkletNode, gain, limiter and analyser, and re-ran addModule for the
  // same processor name. Flick through a few channels and you have a stack of
  // live audio chains fighting over the output.
  TeslaPlayer.prototype.setupAudio = function (s, trak) {
    var self = this;
    if (!this.actx) return;
    var entry = trak.mdia.minf.stbl.stsd.entries[0];
    var desc = ascFromEsds(entry && entry.esds);

    // The sample rate comes from the stream, and it has to be read from the
    // right place. Two things bite here:
    //   - the sample entry's field is `samplerate`, all lowercase, and it is
    //     16.16 FIXED POINT in a uint32, so 48000 arrives as 3145728000;
    //   - the audio TRACK's timescale is the sample rate directly (32000 on
    //     nasa.4k.us, 48000 on the rest), which is the reliable source.
    // Getting this wrong configures the decoder for a rate the data is not,
    // and the channel comes up with a perfect picture and no sound at all.
    var rate = 0;
    if (s.aTrack && s.aTrack.timescale) rate = s.aTrack.timescale;
    if (!rate && entry) {
      var raw = entry.samplerate || entry.samplingRate || 0;
      if (raw > 65536) rate = Math.round(raw / 65536);
      else if (raw) rate = raw;
    }
    if (!rate) rate = (this.actx && this.actx.sampleRate) || 48000;
    s.srcRate = rate;
    // Real channel count too, rather than assuming stereo.
    s.srcCh = (entry && (entry.channel_count || entry.channelCount)) || 2;

    s.adec = new AudioDecoder({
      output: function (data) { self.queueAudio(s, data); data.close(); },
      error: function (e) { s.aErr = 'audio decoder: ' + (e && e.message); s.adec = null; }
    });
    var cfg = { codec: 'mp4a.40.2', sampleRate: s.srcRate, numberOfChannels: s.srcCh };
    if (desc) cfg.description = desc;
    try { s.adec.configure(cfg); } catch (e) { s.aErr = 'audio config: ' + e.message; s.adec = null; return; }
    this.ensureAudio(s.srcRate);
  };

  // Build the ring and its output chain. Idempotent, and shared by every
  // channel. sourceRate tells the ring the stream's rate so it can resample to
  // the device's; channels differ (32kHz vs 48kHz), so it is set per channel.
  TeslaPlayer.prototype.ensureAudio = function (sourceRate) {
    var a = this.audio;
    if (sourceRate) a.sourceRate = sourceRate;
    if (!this.actx) return Promise.resolve(false);
    if (a.ready) { this.setAudioRate(a.sourceRate); return Promise.resolve(true); }
    if (a.pending) return a.pending;
    a.pending = true;
    var self = this, ctx = this.actx;
    var urls = [
      URL.createObjectURL(new Blob([RING_WORKLET], { type: 'application/javascript' })),
      'data:application/javascript;base64,' + btoa(RING_WORKLET)
    ];
    var tryUrl = function (i) {
      if (i >= urls.length) { useScriptProcessor(); return; }
      ctx.audioWorklet.addModule(urls[i]).then(function () {
        wire();
      }).catch(function (e) {
        // Record the refusal rather than swallowing it: a refused module used
        // to leave the page with a perfect picture and no sound at all, with
        // nothing on screen to say why.
        a.modErr += ' [' + (i ? 'data' : 'blob') + ': ' + (e && e.message) + ']';
        tryUrl(i + 1);
      });
    };
    tryUrl(0);

    function wire() {
      a.node = new AudioWorkletNode(ctx, 'lofi-ring', {
        numberOfInputs: 0, numberOfOutputs: 1, outputChannelCount: [2],
        processorOptions: { capacity: Math.max(48000 * 8, ctx.sampleRate * 8),
                            sourceRate: a.sourceRate || ctx.sampleRate }
      });
      a.node.port.onmessage = function (e) {
        var d = e.data || {};
        if (typeof d.depth === 'number') a.depth = d.depth;
        if (typeof d.rms === 'number') a.rms = d.rms;
        if (d.starved) a.underruns = d.underruns;
      };
      a.push = function (planes) { a.node.port.postMessage(planes); };
      a.setRate = function (r) { a.node.port.postMessage({ rate: r }); };
      finish(a.node);
      a.path = 'AudioWorklet';
    }

    // Last resort: the same ring, pulled by a ScriptProcessorNode. Deprecated,
    // but it is a built-in node type - no module loading, no blob URL, so
    // nothing can block it the way a CSP blocks addModule(). It resamples from
    // the stream's rate to the device's, for the same reason the worklet does.
    function useScriptProcessor() {
      try {
        var sp = ctx.createScriptProcessor(2048, 1, 2);
        var cap = Math.max(48000 * 8, ctx.sampleRate * 8);
        var ch = [new Float32Array(cap), new Float32Array(cap)];
        var st = { w: 0, pos: 0, avail: 0, fed: false, under: 0, srcRate: a.sourceRate || ctx.sampleRate };
        st.step = st.srcRate / ctx.sampleRate;
        sp.onaudioprocess = function (ev) {
          var out = ev.outputBuffer, n = out.length, starved = false;
          for (var i = 0; i < n; i++) {
            var l = 0, rr = 0;
            if (st.avail >= 1) {
              var i0 = st.pos | 0, i1 = i0 + 1, f = st.pos - i0;
              var a0 = i0 % cap, a1 = i1 % cap;
              if (f > 0) {
                l = ch[0][a0] + (ch[0][a1] - ch[0][a0]) * f;
                rr = ch[1][a0] + (ch[1][a1] - ch[1][a0]) * f;
              } else { l = ch[0][a0]; rr = ch[1][a0]; }
              st.pos += st.step; st.avail -= st.step;
            } else starved = true;
            out.getChannelData(0)[i] = l;
            if (out.numberOfChannels > 1) out.getChannelData(1)[i] = rr;
          }
          if (starved && st.fed) { st.under++; a.underruns = st.under; }
          a.depth = st.avail / st.srcRate;
        };
        a.push = function (planes) {
          var n = planes[0].length;
          if (st.avail + n > cap) {
            var room = Math.max(0, cap - st.avail);
            for (var i = 0; i < room; i++) {
              ch[0][st.w] = planes[0][i]; ch[1][st.w] = planes[1][i]; st.w = (st.w + 1) % cap;
            }
            st.avail += room;
            return;
          }
          for (var j = 0; j < n; j++) {
            ch[0][st.w] = planes[0][j]; ch[1][st.w] = planes[1][j]; st.w = (st.w + 1) % cap;
          }
          st.avail += n; st.fed = true;
        };
        a.setRate = function (r) {
          st.srcRate = r; st.step = r / ctx.sampleRate;
          st.w = 0; st.pos = 0; st.avail = 0; st.fed = false;
        };
        a.flush = function () { st.w = 0; st.pos = 0; st.avail = 0; st.fed = false; };
        a.node = sp;
        finish(sp);
        a.path = 'ScriptProcessor (fallback)';
      } catch (e) {
        a.path = 'unavailable';
        a.modErr += ' [sproc: ' + e.message + ']';
        a.pending = false;
      }
    }

    function finish(node) {
      // gain -> limiter -> analyser -> destination.
      // The source is quiet (about -30dB mean on these feeds, against roughly
      // -20dB for normal broadcast), so it is lifted back to programme level.
      // The limiter is what stops peaks that were already near -10dB from
      // clipping once they are amplified.
      var g = ctx.createGain();
      g.gain.value = self.gainValue;
      var lim = ctx.createDynamicsCompressor();
      lim.threshold.value = -6; lim.knee.value = 6; lim.ratio.value = 12;
      lim.attack.value = 0.003; lim.release.value = 0.25;
      var an = ctx.createAnalyser();
      an.fftSize = 2048;
      node.connect(g); g.connect(lim); lim.connect(an); an.connect(ctx.destination);
      a.gain = g; a.limiter = lim; a.analyser = an; a.buf = new Float32Array(2048);
      a.ready = true; a.pending = false;
      if (ctx.state === 'suspended') ctx.resume().catch(function () {});
      a.pending = Promise.resolve(true);
    }
    return a.pending;
  };

  // Tell the ring which rate the incoming samples are in. Sent when a channel
  // opens, because channels differ: the ring drains at the device's rate, so a
  // mismatch starves it continuously.
  TeslaPlayer.prototype.setAudioRate = function (r) {
    var a = this.audio;
    if (!r || a.rate === r) return;
    a.rate = r;
    if (a.setRate) a.setRate(r);
  };

  // Drop whatever the previous channel left in the ring, so a switch does not
  // play out the tail of the old one.
  TeslaPlayer.prototype.flushAudio = function () {
    var a = this.audio;
    if (a.flush) a.flush();
    else if (a.node && a.node.port) { try { a.node.port.postMessage({ flush: 1 }); } catch (e) {} }
    a.underruns = 0; a.depth = 0;
  };

  TeslaPlayer.prototype.queueAudio = function (s, data) {
    if (s.stopped) return;
    var a = this.audio;
    if (!a.ready || !a.push) return;
    var n = data.numberOfFrames;
    var planes = [new Float32Array(n), new Float32Array(n)];
    var nch = data.numberOfChannels || 1;
    for (var c = 0; c < 2; c++) {
      data.copyTo(planes[c], { planeIndex: c < nch ? c : 0, format: 'f32-planar' });
    }
    a.push(planes);
    s.aFrames++;
    // Counted in the track's own timebase, so a 32kHz channel reports seconds
    // rather than being read as if it were 48kHz.
    s.aSeconds += n / (data.sampleRate || s.srcRate || 48000);
  };

  // ---- samples ------------------------------------------------------------
  TeslaPlayer.prototype.onSamples = function (s, id, samples) {
    s.samples += samples.length;
    // Every append begins at a segment boundary, and HLS segments open with a
    // keyframe even when the packager omits mp4box's is_sync flags. Without
    // this the decoder key-waits and stalls.
    var first = true;
    for (var i = 0; i < samples.length; i++) {
      var sm = samples[i];
      if (!sm || !sm.data || !sm.data.byteLength) continue;
      var raw = new Uint8Array(sm.data.buffer.slice(sm.data.byteOffset, sm.data.byteOffset + sm.data.byteLength));
      var sync = !!sm.is_sync || first;
      first = false;
      if (s.vTrack && id === s.vTrack.id) {
        var vs = (s.vTrack.timescale || 90000);
        var sec = sm.cts / vs;
        // Anchor the media clock so the first frame is due right now. Joining
        // mid-stream means the timestamps already read tens of seconds; anchored
        // at t=0 every frame would be due 40s in the future and nothing would
        // ever decode.
        if (s.vT0 < 0) s.vT0 = Date.now() - sec * 1000;
        if (sec > s.mediaEnd) s.mediaEnd = sec;
        s.pend.push({ sync: sync, ts: Math.round(sec * 1e6), due: s.vT0 + sec * 1000, raw: raw });
      } else if (s.aTrack && s.adec && id === s.aTrack.id) {
        // Audio gets its own queue, paced on the same media clock. Sharing the
        // video queue lets audio evict video - AAC frames outnumber video ~2:1 -
        // and feeding it unpaced delivers a whole segment in one burst, which
        // runs the scheduler far ahead of the audio clock and stutters.
        var as = (s.aTrack.timescale || 48000);
        var asec = sm.cts / as;
        s.aPend.push({ sync: sync, ts: Math.round(asec * 1e6), due: s.vT0 >= 0 ? s.vT0 + asec * 1000 : 0, raw: raw });
      }
    }
    // Cap each queue at the WORST-CASE buffer, not the target. Fetching happens
    // a whole segment at a time, so the buffer oscillates between the target and
    // target+segment; capping at the target overflowed on every segment and the
    // overflow drops the OLDEST entries - the frames about to be shown, which
    // shows up as multi-second stalls.
    while (s.pend.length > s.caps.v) { s.pend.shift(); s.drops++; }
    while (s.aPend.length > s.caps.a) { s.aPend.shift(); s.drops++; }
  };

  // Seconds of downloaded media ahead of what is playing right now. Negative
  // means we have run past the download and are starved.
  function bufferedOf(s) {
    if (s.vT0 < 0 || !s.mediaEnd) return 0;
    return s.mediaEnd - (Date.now() - s.vT0) / 1000;
  }

  // Feed the decoders on the media clock. Handing every sample straight to
  // decode() lets the decoder sprint through a whole segment in milliseconds
  // and dump it at once, so the paint gate almost never finds a frame waiting.
  TeslaPlayer.prototype.feedDecoders = function (s, now) {
    if (!s.vdec || s.vdec.state !== 'configured') return;
    while (s.pend.length && s.pend[0].due < now - 500) { s.pend.shift(); s.drops++; }
    while (s.pend.length && s.pend[0].due - now < 250) {
      if (s.vdec.decodeQueueSize > 6) break;
      var c = s.pend.shift();
      try {
        s.vdec.decode(new EncodedVideoChunk({ type: c.sync ? 'key' : 'delta', timestamp: c.ts, data: c.raw }));
      } catch (e) { /* queue full */ }
    }
    // Audio is released further ahead of its due time than video, because a ring
    // buffer only smooths jitter if it actually holds a cushion. Releasing it
    // just before it is due leaves the ring at zero and turns every hiccup into
    // a dropout.
    if (s.adec && s.adec.state === 'configured') {
      while (s.aPend.length && s.aPend[0].due && s.aPend[0].due < now - 500) { s.aPend.shift(); s.drops++; }
      while (s.aPend.length && s.aPend[0].due - now < 1500) {
        if (s.adec.decodeQueueSize > 40) break;
        var a = s.aPend.shift();
        try {
          s.adec.decode(new EncodedAudioChunk({ type: a.sync ? 'key' : 'delta', timestamp: a.ts, data: a.raw }));
        } catch (e) {}
      }
    }
  };

  // ---- video frames -------------------------------------------------------
  TeslaPlayer.prototype.onVideoFrame = function (s, frame) {
    if (s.stopped) { try { frame.close(); } catch (e) {} return; }
    s.frames++;
    var sec = (frame.timestamp || 0) / 1e6;
    var due = s.vT0 >= 0 ? s.vT0 + sec * 1000 : 0;
    // A short queue, not a single slot. The decoder runs slightly ahead of the
    // wall clock, so the NEWEST frame is always a few ms in the future; keeping
    // only that one meant every paint check found "not due yet" and nothing was
    // ever shown.
    //
    // The cap has to exceed the decoder's lookahead or the overflow discards
    // the frames that were due next and the picture skips. With a 250ms lead at
    // 60fps that is ~15 frames, so the cap scales with the learned frame rate.
    if (s.lastFrameDue >= 0 && due > s.lastFrameDue) {
      // The source's real frame interval, as a rolling MEDIAN of the gaps
      // between consecutive decoded frames.
      //
      // A plain exponential average is not safe here. One outlier - a
      // timestamp gap across a segment boundary, or a B-frame reordering - pulls
      // the estimate far enough that anything derived from it moves too, and
      // when that something is a queue cap the queue starts shedding the frames
      // it is supposed to be holding. The median ignores outliers by
      // construction, so a single bad gap cannot resize anything.
      var d = due - s.lastFrameDue;
      s.ivWin.push(d);
      if (s.ivWin.length > 60) s.ivWin.shift();
      if (s.ivWin.length > 8) {
        var sorted = s.ivWin.slice().sort(function (a, b) { return a - b; });
        s.frameIv = sorted[sorted.length >> 1];
      }
    }
    s.lastFrameDue = due;
    s.fq.push({ frame: frame, due: due });
    while (s.fq.length > s.caps.fq) { s.fq.shift().frame.close(); s.drops++; }
  };

  // ---- paint --------------------------------------------------------------
  // Painting is driven by a timer aimed at each frame's own due time, not by
  // rAF. rAF only fires about every 16.7ms, so a 25fps source (a frame every
  // 40ms) is always caught at the NEXT rAF after it comes due - a fixed ~10ms
  // late, every frame, forever. That is not judder you can filter out later; it
  // is a 20fps picture no matter what the source says.
  TeslaPlayer.prototype.schedulePaint = function (s, gen) {
    var self = this;
    if (s.paintTimer || s.stopped) return;
    if (!s.fq.length) {
      s.paintTimer = setTimeout(function () { s.paintTimer = null; self.schedulePaint(s, gen); }, 10);
      return;
    }
    var wait = Math.max(0, s.fq[0].due - Date.now());
    s.paintTimer = setTimeout(function () {
      s.paintTimer = null;
      if (s.stopped || self.gen !== gen) return;
      self.paintOne(s);
      self.schedulePaint(s, gen);
    }, wait);
  };

  TeslaPlayer.prototype.paintOne = function (s) {
    if (!s.fq.length) return;
    var wall = Date.now();
    // Oldest DUE frame, first-in-first-out. Taking the newest discards
    // everything queued before it, which is a silent drop.
    if (s.fq[0].due && s.fq[0].due > wall) return;
    var box = s.fq.shift();
    var frame = box.frame;
    var w = frame.displayWidth || frame.codedWidth;
    var h = frame.displayHeight || frame.codedHeight;
    if (!w || !h) { try { frame.close(); } catch (e) {} return; }
    s.width = w; s.height = h;
    if (this.canvas.width !== w || this.canvas.height !== h) {
      this.canvas.width = w; this.canvas.height = h;
    }
    this.ctx2d.drawImage(frame, 0, 0, this.canvas.width, this.canvas.height);
    try { frame.close(); } catch (e) {}
    s.drawn++;
    if (!s.firstFrameAt) s.firstFrameAt = wall;
    if (s.lastGapAt) {
      var gap = wall - s.lastGapAt;
      s.gaps.push(gap);
      if (s.gaps.length > 400) s.gaps.shift();
    }
    s.lastGapAt = wall;
    s.lastPaintT = wall;
    this.updateFps(s, wall);
  };

  TeslaPlayer.prototype.updateFps = function (s, wall) {
    var dt = s.lastDecT ? (wall - s.lastDecT) / 1000 : 0;
    s.decFps = s.decFps ? s.decFps * 0.7 + (1 / dt) * 0.3 : (dt ? 1 / dt : 0);
    s.paintFps = s.paintFps ? s.paintFps * 0.7 + (1 / dt) * 0.3 : (dt ? 1 / dt : 0);
    s.lastDecT = wall;
  };

  // ---- segment pump -------------------------------------------------------
  // The live window slides: old segments fall off the front of the playlist as
  // new ones appear, so position is tracked by URL, never by index. And the
  // download is paced to buffer depth, not to a fixed clock - decode runs far
  // faster than real time, so an unpaced loop chews through the whole window in
  // seconds and re-downloads the same segments forever.
  TeslaPlayer.prototype.reanchor = function (s) {
    var i = -1;
    for (var k = 0; k < s.segs.length; k++) { if (s.segs[k].url === s.lastUrl) { i = k; break; } }
    // -1 means our last segment has already slid out of the window.
    s.cursor = i >= 0 ? i + 1 : Math.max(0, s.segs.length - 2);
  };

  var sleep = function (ms) { return new Promise(function (r) { setTimeout(r, ms); }); };

  TeslaPlayer.prototype.pump = function (s, gen) {
    var self = this;
    if (s.pumpRunning) return;
    s.pumpRunning = true;
    (function loop() {
      if (s.stopped || self.gen !== gen) { s.pumpRunning = false; return; }
      if (s.cursor >= s.segs.length) {
        fetch(s.base, { cache: 'no-store' }).then(function (r) {
          return r.ok ? r.text() : Promise.reject(new Error('HTTP ' + r.status));
        }).then(function (t) {
          var p = parsePlaylist(t);
          if (p.segs.length) { s.segs = p.segs; s.target = p.target; s.segDur = p.segs[0].dur; }
          self.reanchor(s);
          return sleep(0);
        }).catch(function (e) { return sleep(1500); }).then(function () { loop(); });
        return;
      }
      // Pace by BUFFER DEPTH. One segment per segment-duration means media
      // arrives exactly when it is needed, so the audio ring sits at zero and
      // every hiccup becomes a dropout.
      if (bufferedOf(s) > BUFFER_TARGET_S) { sleep(200).then(loop); return; }
      var seg = s.segs[s.cursor];
      if (!seg) { sleep(100).then(loop); return; }
      s.cursor++; s.lastUrl = seg.url;
      fetch(absolute(seg.url, s.base), { cache: 'no-store' }).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.arrayBuffer();
      }).then(function (buf) {
        if (s.stopped || self.gen !== gen) return;
        s.segsFetched++;
        s.tm.push(new Uint8Array(buf));
        // push() only feeds the parser. flush() is what tells mux.js the segment
        // ended, and that is what makes it remux and emit 'data' - without it
        // the transmuxer just buffers and nothing ever comes out.
        s.tm.flush();
      }).catch(function (e) {
        s.fetchErr = e.message;
      }).then(function () { loop(); });
    })();
  };

  // ---- teardown -----------------------------------------------------------
  // Without this every channel change leaks a VideoDecoder, an AudioDecoder, a
  // paint timer and a pump loop. Flick through a dozen channels and the page
  // is running a dozen pipelines at once.
  TeslaPlayer.prototype.close = function () {
    var s = this.live;
    if (!s) return;
    s.stopped = true;
    this.gen++;                      // orphan every in-flight callback
    this.live = null;
    this.emit('state', this.state());
    try { if (s.vdec && s.vdec.state !== 'closed') s.vdec.close(); } catch (e) {}
    try { if (s.adec && s.adec.state !== 'closed') s.adec.close(); } catch (e) {}
    // The audio graph deliberately SURVIVES a channel switch - it belongs to the
    // player, not the stream. Only the ring's contents are dropped, so the next
    // channel does not play out the tail of this one.
    this.flushAudio();
    try { if (s.tm) s.tm.removeAllListeners(); } catch (e) {}
    try { if (s.mbox) s.mbox.stop(); } catch (e) {}
    try { if (s.paintTimer) clearTimeout(s.paintTimer); } catch (e) {}
    try { if (s.pumpTimer) clearInterval(s.pumpTimer); } catch (e) {}
    try { if (s.statsTimer) clearInterval(s.statsTimer); } catch (e) {}
    try { if (s.rafId) cancelAnimationFrame(s.rafId); } catch (e) {}
    // Release any frames still queued. VideoFrames hold GPU/system memory and
    // an unclosed frame is a leak the GC will not collect.
    for (var i = 0; i < s.fq.length; i++) { try { s.fq[i].frame.close(); } catch (e) {} }
    s.fq.length = 0;
    s.pend.length = 0; s.aPend.length = 0;
    this.ctx2d.fillStyle = '#000';
    this.ctx2d.fillRect(0, 0, this.canvas.width, this.canvas.height);
  };

  // Tear the persistent audio graph down. Only needed when leaving Drive mode
  // for good - a channel switch must NOT do this, or every switch would rebuild
  // the whole chain and re-register the processor name.
  TeslaPlayer.prototype.destroy = function () {
    this.close();
    var a = this.audio;
    try { if (a.node) { if (a.node.port) a.node.port.onmessage = null; a.node.disconnect(); } } catch (e) {}
    try { if (a.gain) a.gain.disconnect(); } catch (e) {}
    try { if (a.limiter) a.limiter.disconnect(); } catch (e) {}
    try { if (a.analyser) a.analyser.disconnect(); } catch (e) {}
    if (this.actx) { try { this.actx.close(); } catch (e) {} }
    this.actx = null;
    this.audio = { ready: false, pending: null, node: null, push: null, flush: null,
                   setRate: null, rate: 0, sourceRate: 0,
                   path: '', modErr: '', depth: 0, rms: 0, underruns: 0,
                   gain: null, limiter: null, analyser: null, buf: null };
  };

  TeslaPlayer.prototype.fail = function (msg) {
    if (this.live) { this.live.fatal = msg; this.emit('note', msg); }
    if (this.listeners.fail) this.emit('fail', msg);
  };

  // ---- reporting ----------------------------------------------------------
  TeslaPlayer.prototype.state = function () {
    var s = this.live;
    if (!s) return { stage: 'idle' };
    var st = {
      stage: s.fatal ? 'error' : (s.firstFrameAt ? 'live' : 'starting'),
      channel: s.meta.name || '',
      url: s.url,
      master: !!s.master,
      codec: s.codec, width: s.width, height: s.height,
      srcRate: s.srcRate, srcCh: s.srcCh || 2, fps: s.frameIv ? (1000 / s.frameIv) : 0,
      drawn: s.drawn, drops: s.drops, segs: s.segsFetched,
      audioPath: this.audio.path, modErr: this.audio.modErr,
      ctxState: this.actx ? this.actx.state : 'none',
      aFrames: s.aFrames, aSeconds: s.aSeconds, aUnder: this.audio.underruns,
      aQueue: this.audio.depth, aRms: this.audio.rms,
      outRms: this.outRms(),
      buffered: bufferedOf(s),
      fatal: s.fatal, fetchErr: s.fetchErr, aErr: s.aErr || '',
      firstFrameMs: s.firstFrameAt ? (s.firstFrameAt - s.t0) : 0
    };
    // The paint interval is the number that says whether the picture is smooth.
    // 40ms for a 25fps source is perfect; 50ms means judder.
    if (s.gaps.length > 4) {
      var g = s.gaps.slice().sort(function (a, b) { return a - b; });
      st.medGap = Math.round(g[Math.floor(g.length / 2)]);
      st.worstGap = Math.round(g[Math.floor(g.length * 0.95)]);
      st.spread = Math.round(g[Math.floor(g.length * 0.9)] - g[Math.floor(g.length / 2)]);
    }
    st.canvW = this.canvas.width; st.canvH = this.canvas.height;
    return st;
  };

  // Level actually leaving the graph, measured after the gain and the limiter.
  // Reading it before the gain would say nothing about audibility.
  TeslaPlayer.prototype.outRms = function () {
    var a = this.audio;
    if (!a.analyser || !a.buf) return 0;
    a.analyser.getFloatTimeDomainData(a.buf);
    var sum = 0;
    for (var i = 0; i < a.buf.length; i++) sum += a.buf[i] * a.buf[i];
    return Math.sqrt(sum / a.buf.length);
  };

  global.TeslaPlayer = TeslaPlayer;
  global.TeslaEngine = {
    missingLibs: missingLibs,
    RING_WORKLET: RING_WORKLET
  };
})(window);
