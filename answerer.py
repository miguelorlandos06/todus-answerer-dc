#!/usr/bin/env python3
"""Answerer ToDus: acepta CALL, pide TURN, recibe OFFER, responde ANSWER con DataChannel."""
import os, re, ssl, base64, hashlib, socket, threading, time, uuid, json, asyncio, logging, fractions
import requests, urllib3
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCConfiguration, RTCIceServer
from aiortc.mediastreams import AudioStreamTrack, VideoStreamTrack
from av import AudioFrame, VideoFrame

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PHONE       = os.environ.get("TODUS_PHONE", "5351417372")
AUTH_URL    = "https://auth.todus.cu/v2/auth/token"
XMPP_HOST   = os.environ.get("XMPP_HOST", "ws.todus.cu")
XMPP_PORT   = int(os.environ.get("XMPP_PORT", "1756"))
XMPP_DOMAIN = "im.todus.cu"
FAKE_SECRET = "00000000-0000-0000-0000-000000000000"
RECV_BUFFER = 65536
RUNTIME     = int(os.environ.get("RUNTIME", "300"))

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S")
log = logging.getLogger("ans")


def _varint(n):
    out = b""
    while True:
        b = n & 0x7F; n >>= 7
        if n: out += bytes([b | 0x80])
        else: out += bytes([b]); break
    return out

def _sf(num, val):
    if isinstance(val, str): val = val.encode()
    return bytes([(num << 3) | 2]) + _varint(len(val)) + val

def login(phone):
    phone = re.sub(r"[^\d]", "", phone)
    payload = _sf(1, phone) + _sf(2, FAKE_SECRET)
    r = requests.post(AUTH_URL, data=payload, timeout=30, verify=False,
        headers={"Content-Type": "application/x-protobuf", "User-Agent": "ToDus 2.1.2 Auth"})
    r.raise_for_status()
    m = re.search(r"eyJ[\w\-\.]+", r.content.decode("utf-8", "ignore"))
    if not m: raise RuntimeError(f"No JWT: {r.content[:200]}")
    return m.group(0)

def b64(s):
    if isinstance(s, str): s = s.encode()
    return base64.b64encode(s).decode()

def decode_b64(s):
    if not s: return ""
    try:
        pad = "=" * (-len(s) % 4)
        return base64.b64decode(s + pad).decode("utf-8", "replace")
    except Exception:
        return s


class SilenceTrack(AudioStreamTrack):
    kind = "audio"
    def __init__(self):
        super().__init__(); self._ts = 0
    async def recv(self):
        await asyncio.sleep(0.02)
        f = AudioFrame(format="s16", layout="mono", samples=960)
        f.pts = self._ts; f.sample_rate = 48000
        for p in f.planes: p.update(bytes(1920))
        self._ts += 960
        return f


class BlackVideoTrack(VideoStreamTrack):
    kind = "video"
    def __init__(self):
        super().__init__(); self._ts = 0
    async def recv(self):
        await asyncio.sleep(1/30)
        f = VideoFrame(width=320, height=240, format="yuv420p")
        for p in f.planes: p.update(bytes(p.buffer_size))
        f.pts = self._ts; f.time_base = fractions.Fraction(1, 90000)
        self._ts += 3000
        return f


class Answerer:
    def __init__(self, phone):
        self.phone   = re.sub(r"[^\d]", "", phone)
        self.jwt     = None
        self.sock    = None
        self.buf     = b""
        self.send_lock = threading.Lock()
        self.running = True
        self.full_jid = f"{self.phone}@{XMPP_DOMAIN}"
        self.peer_jid = None
        self.call_id  = None
        self.pc       = None
        self.dc       = None
        self.answer_sent = False
        self.dc_opened = False
        self._loop    = None
        self.phase    = "init"
        # TURN
        self._turn_iq_id  = None
        self._turn_response = None
        self._turn_event = threading.Event()

    def send(self, data, label=""):
        with self.send_lock:
            raw = data.encode() if isinstance(data, str) else data
            log.info(f">>> [{label}] {raw[:400]!r}")
            try: self.sock.sendall(raw)
            except Exception as e: log.warning(f"send fallo: {e}")

    def recv_until(self, marker, timeout=10):
        self.sock.settimeout(timeout)
        data = b""
        try:
            while marker not in data:
                c = self.sock.recv(RECV_BUFFER)
                if not c: break
                data += c
        except socket.timeout: pass
        return data

    def conectar(self):
        log.info("=" * 70)
        log.info(f"LOGIN {self.phone}")
        self.jwt = login(self.phone)
        log.info("JWT OK")
        resource = hashlib.md5(self.phone.encode()).hexdigest() + "_ans_" + uuid.uuid4().hex[:8]
        ctx = ssl.create_default_context()
        ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
        raw = socket.create_connection((XMPP_HOST, XMPP_PORT), timeout=30)
        self.sock = ctx.wrap_socket(raw, server_hostname=XMPP_HOST)
        self.sock.settimeout(30)
        self.send('<?xml version="1.0"?><stream:stream to="%s" xmlns="jc" xmlns:stream="x1" version="1.0">' % XMPP_DOMAIN, "STREAM-1")
        self.recv_until(b"</stream:features>")
        auth = b"\x00" + self.phone.encode() + b"\x00" + self.jwt.encode()
        self.send(f'<auth mechanism="PLAIN" xmlns="urn:ietf:params:xml:ns:xmpp-sasl">{b64(auth)}</auth>', "AUTH")
        r = self.recv_until(b"<ok", timeout=10)
        if b"<ok" not in r and b"<success" not in r:
            raise RuntimeError(f"SASL fallo: {r[:300]}")
        self.send('<?xml version="1.0"?><stream:stream to="%s" xmlns="jc" xmlns:stream="x1" version="1.0">' % XMPP_DOMAIN, "STREAM-2")
        self.recv_until(b"</stream:features>")
        bid = uuid.uuid4().hex
        self.send(f'<iq type="set" id="{bid}"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><resource>{resource}</resource></bind></iq>', "BIND")
        r = self.recv_until(b"</iq>", timeout=10)
        m = re.search(rb"<jid>([^<]+)</jid>", r)
        if m: self.full_jid = m.group(1).decode()
        self.send('<iq type="set" id="sess1"><session xmlns="urn:ietf:params:xml:ns:xmpp-session"/></iq>', "SESSION")
        self.recv_until(b"</iq>", timeout=10)
        self.send("<presence/>", "PRESENCE")
        time.sleep(0.3)
        log.info("=" * 70)
        log.info(f"CONECTADO: {self.full_jid}")
        log.info(f"Esperando CALL en {self.phone}")
        log.info("=" * 70)

    def reader(self):
        self.sock.settimeout(1)
        while self.running:
            try:
                c = self.sock.recv(RECV_BUFFER)
                if not c: log.warning("SOCKET CERRADO"); return
                self.buf += c
                self._process()
            except socket.timeout: continue
            except Exception as e:
                log.warning(f"reader: {e}"); return

    def keepalive(self):
        while self.running:
            time.sleep(25)
            try: self.send(" ", "KEEPALIVE")
            except Exception: return

    def _update_peer(self, stanza):
        m = re.search(r"\bf=['\"]([^'\"]+)['\"]", stanza)
        if m and "/" in m.group(1):
            new_peer = m.group(1)
            if new_peer != self.peer_jid:
                log.info(f"*** peer_jid FULL: {new_peer}")
                self.peer_jid = new_peer

    def _process(self):
        while True:
            text = self.buf.decode("utf-8", "replace")
            if not text: return
            i = text.find("<m ")
            if i == 0:
                end = text.find("</m>", i)
                if end == -1:
                    end = text.find("/>", i)
                    if end == -1: return
                    end += 2
                else: end += 4
                stanza = text[i:end]
                self.buf = self.buf[end:]
                self._on_message(stanza)
                continue
            i = text.find("<iq")
            if i == 0:
                end = text.find("</iq>", i)
                if end == -1:
                    sc = re.match(r"<iq\b[^>]*/>", text)
                    if sc:
                        stanza = sc.group(0)
                        self.buf = self.buf[len(stanza):]
                        self._on_iq(stanza)
                        continue
                    return
                stanza = text[:end + 5]
                self.buf = self.buf[end + 5:]
                self._on_iq(stanza)
                continue
            next_m = text.find("<m ")
            next_iq = text.find("<iq")
            candidates = [x for x in (next_m, next_iq) if x != -1]
            if not candidates: self.buf = b""; return
            self.buf = self.buf[min(candidates):]

    def _on_message(self, stanza):
        log.info(f"\n<<< MSG RAW: {stanza[:600]}")
        self._update_peer(stanza)
        m_tcall = re.search(r"<tcall[^>]*?st=['\"]([^'\"]+)['\"][^>]*?cid=['\"]([^'\"]+)['\"]", stanza)
        if not m_tcall:
            m_tcall = re.search(r"<tcall[^>]*?cid=['\"]([^'\"]+)['\"][^>]*?st=['\"]([^'\"]+)['\"]", stanza)
            if not m_tcall: return
            cid, st_raw = m_tcall.group(1), m_tcall.group(2)
        else:
            st_raw, cid = m_tcall.group(1), m_tcall.group(2)
        st_dec = decode_b64(st_raw)
        cmd = st_dec.split(" ", 1)[0]
        log.info(f"*** TCALL cmd={cmd} cid={cid} peer={self.peer_jid}")

        if cmd == "CALL":
            self.call_id = cid
            self.phase = "picked_sent"
            threading.Thread(target=self._aceptar, daemon=True).start()
        elif cmd == "END":
            log.warning("*** PEER END")

    def _aceptar(self):
        if not self.peer_jid: return
        time.sleep(0.4)
        msg_id = uuid.uuid4().hex[:16]
        tc_id  = uuid.uuid4().hex[:16]
        self.send(
            f'<m to="{self.peer_jid}" t="c" i="{msg_id}" xmlns="jc">'
            f'<k xmlns="x8"/>'
            f'<tcall xmlns="tcall:n" i="{tc_id}" mi="{msg_id}" '
            f'st="{b64("PICKED")}" cid="{self.call_id}"/>'
            f'<b>PICKED</b></m>', "PICKED"
        )
        log.info("*** PICKED enviado")

    def _on_iq(self, stanza):
        # ── Respuesta a nuestro GET-TURN ──
        if self._turn_iq_id:
            m_id = re.search(r"\bi(?:d)?=['\"]([^'\"]+)['\"]", stanza)
            if m_id and m_id.group(1) == self._turn_iq_id and "todus:turn:cred" in stanza:
                self._handle_turn_response(stanza)
                return

        if "todus:call:status" not in stanza: return
        q = re.search(r"status=['\"]([^'\"]+)['\"]", stanza)
        if not q: return
        status = q.group(1)
        log.info(f"\n<<< IQ STATUS={status}")
        self._update_peer(stanza)

        cm = re.search(r"content=['\"]([^'\"]+)['\"]", stanza)
        if not cm: return
        try:
            content_json = decode_b64(cm.group(1))
            parsed = json.loads(content_json)
            state_dec = decode_b64(parsed.get("state", ""))
            parts = state_dec.split(" ", 1)
            cmd = parts[0]
            payload = parts[1] if len(parts) > 1 else ""
            log.info(f"    cmd={cmd} payload_len={len(payload)}")

            if cmd == "OFFER" and not self.answer_sent:
                self.phase = "offer_received"
                asyncio.run_coroutine_threadsafe(self._responder_offer(payload), self._loop)
            elif cmd == "ICE":
                asyncio.run_coroutine_threadsafe(self._apply_ice(payload), self._loop)
            elif cmd == "END":
                log.warning("*** IQ END")
                self.phase = "peer_ended_iq"
        except Exception as e:
            log.warning(f"parse IQ: {e}")

    # ───────── TURN ─────────
    def pedir_turn(self):
        self._turn_iq_id = uuid.uuid4().hex
        self._turn_response = None
        self._turn_event.clear()
        self.send(
            f'<iq type="get" id="{self._turn_iq_id}" to="{XMPP_DOMAIN}">'
            f'<query xmlns="todus:turn:cred"/>'
            f'</iq>', "GET-TURN"
        )
        if not self._turn_event.wait(timeout=15):
            log.warning("*** TURN: timeout")
            return None
        return self._turn_response

    def _handle_turn_response(self, stanza):
        m = re.search(r"<query[^>]*>([^<]+)</query>", stanza)
        if not m:
            log.warning("*** TURN: sin contenido")
            self._turn_event.set(); return
        try:
            b64_content = m.group(1).strip()
            pad = "=" * (-len(b64_content) % 4)
            decoded = base64.b64decode(b64_content + pad).decode("utf-8")
            turn = json.loads(decoded)
            log.info(f"*** TURN obtenido: user={turn.get('username')} ttl={turn.get('ttl')}")
            for u in turn.get("uris", []):
                log.info(f"    {u['proto']}://{u['uri']}")
            self._turn_response = turn
        except Exception as e:
            log.warning(f"*** TURN: error parseando: {e}")
        finally:
            self._turn_event.set()

    def _build_ice_servers(self, turn):
        if not turn or not turn.get("uris"):
            return []
        urls = [f"{u['uri']}?transport={u['proto']}" for u in turn["uris"]]
        return [RTCIceServer(
            urls=urls,
            username=turn.get("username", ""),
            credential=turn.get("password", ""),
        )]

    # ───────── ANSWER ─────────
    async def _responder_offer(self, offer_sdp):
        try:
            mlines = re.findall(r"^m=(\w+)", offer_sdp, re.MULTILINE)
            log.info(f"*** SDP [OFFER-REMOTE] m-lines: {mlines}")
            if "application" in mlines:
                log.info("*** ✅ OFFER-REMOTE CONTIENE m=application")

            log.info("*** Pidiendo TURN a ToDus...")
            turn = self.pedir_turn()
            ice_servers = self._build_ice_servers(turn)
            if ice_servers:
                log.info(f"*** TURN configurado: {ice_servers[0].urls}")
            else:
                log.warning("*** Sin TURN, ICE podría fallar")

            log.info("*** Creando RTCPeerConnection + DataChannel")
            self.pc = RTCPeerConnection(configuration=RTCConfiguration(iceServers=ice_servers))

            @self.pc.on("icecandidate")
            async def on_ice(cand):
                if cand: await self._enviar_ice(cand)

            @self.pc.on("connectionstatechange")
            async def on_state():
                log.info(f"*** PC state: {self.pc.connectionState}")

            @self.pc.on("iceconnectionstatechange")
            async def on_ice_state():
                log.info(f"*** ICE state: {self.pc.iceConnectionState}")

            self.pc.addTrack(SilenceTrack())
            self.pc.addTrack(BlackVideoTrack())
            self.dc = self.pc.createDataChannel("tunel", ordered=True)
            log.info(f"*** DataChannel creado: {self.dc.label}")

            @self.dc.on("open")
            def on_open():
                log.info("=" * 60)
                log.info("*** DATA CHANNEL ABIERTO ***")
                log.info("=" * 60)
                self.dc_opened = True
                asyncio.create_task(self._test_send())

            @self.dc.on("message")
            def on_msg(msg):
                log.info(f"*** DC recibido: {msg[:200]!r}")

            @self.dc.on("close")
            def on_close():
                log.info("*** DC cerrado")

            offer = RTCSessionDescription(sdp=offer_sdp, type="offer")
            await self.pc.setRemoteDescription(offer)
            answer = await self.pc.createAnswer()
            await self.pc.setLocalDescription(answer)

            amlines = re.findall(r"^m=(\w+)", answer.sdp, re.MULTILINE)
            log.info(f"*** SDP [ANSWER-LOCAL] m-lines: {amlines}")
            if "application" in amlines:
                log.info("*** ✅ ANSWER-LOCAL CONTIENE m=application")

            await self._enviar_answer(answer.sdp)
            self.phase = "answer_sent"
        except Exception as e:
            log.exception(f"responder_offer: {e}")

    async def _test_send(self):
        try:
            for i in range(30):
                msg = f"pong_{i}"
                self.dc.send(msg)
                log.info(f"*** DC enviado: {msg}")
                await asyncio.sleep(1)
        except Exception as e:
            log.warning(f"test_send: {e}")

    async def _enviar_answer(self, sdp):
        state_b64 = b64(f"ANSWER {sdp}")
        content = json.dumps({
            "callId": self.call_id, "endTime": -1,
            "msgId": f"call_{uuid.uuid4().hex}", "startTime": -1,
            "state": state_b64, "userId": self.phone + "@im.todus.cu", "type": 32,
        })
        if "/" not in self.peer_jid:
            log.warning(f"*** ⚠️ ANSWER peer_jid SIN RESOURCE: {self.peer_jid}")
        else:
            log.info(f"*** peer_jid FULL confirmado: {self.peer_jid}")
        iq_id = uuid.uuid4().hex
        self.send(
            f'<iq type="set" id="{iq_id}" to="{self.peer_jid}" from="{self.full_jid}">'
            f'<query xmlns="todus:call:status" '
            f'to_user="{self.peer_jid}" from_user="{self.full_jid}" '
            f'status="ANSWER" content="{b64(content)}"/>'
            f'</iq>', "ANSWER"
        )
        self.answer_sent = True
        log.info("*** ANSWER enviado")

    async def _enviar_ice(self, candidate):
        payload = f"{candidate.sdpMid} {candidate.sdpMLineIndex} {candidate.candidate}"
        state_b64 = b64(f"ICE {payload}")
        content = json.dumps({
            "callId": self.call_id, "endTime": -1,
            "msgId": f"call_{uuid.uuid4().hex}", "startTime": -1,
            "state": state_b64, "userId": self.phone + "@im.todus.cu", "type": 32,
        })
        if "/" not in self.peer_jid:
            log.warning(f"*** ⚠️ ICE peer_jid SIN RESOURCE: {self.peer_jid}")
        iq_id = uuid.uuid4().hex
        self.send(
            f'<iq type="set" id="{iq_id}" to="{self.peer_jid}" from="{self.full_jid}">'
            f'<query xmlns="todus:call:status" '
            f'to_user="{self.peer_jid}" from_user="{self.full_jid}" '
            f'status="ICE" content="{b64(content)}"/>'
            f'</iq>', "ICE"
        )

    async def _apply_ice(self, payload):
        parts = payload.split(" ", 2)
        if len(parts) != 3:
            log.warning(f"ICE mal formado: {payload[:100]!r}"); return
        mid, mline, cand = parts
        try:
            from aiortc.sdp import candidate_from_sdp
            if not mline.isdigit():
                log.warning(f"ICE mline no numérico: {mline!r}"); return
            candidate = candidate_from_sdp(cand)
            candidate.sdpMid = mid
            candidate.sdpMLineIndex = int(mline)
            if self.pc:
                await self.pc.addIceCandidate(candidate)
                log.info(f"*** ICE remote aplicado (mline={mline})")
        except Exception as e:
            log.warning(f"ICE remote: {e}")

    def close(self):
        self.running = False
        try:
            if self.dc: self.dc.close()
        except Exception: pass
        try:
            if self.pc: asyncio.run_coroutine_threadsafe(self.pc.close(), self._loop)
        except Exception: pass
        try:
            if self.sock:
                self.send("</stream:stream>", "CLOSE")
                self.sock.close()
        except Exception: pass


async def main_async():
    loop = asyncio.get_running_loop()
    bot = Answerer(PHONE)
    bot._loop = loop
    bot.conectar()
    threading.Thread(target=bot.reader, daemon=True).start()
    threading.Thread(target=bot.keepalive, daemon=True).start()
    log.info(f"LISTO - escuchando en {PHONE} por {RUNTIME}s")
    try:
        await asyncio.sleep(RUNTIME)
    finally:
        log.info("=" * 70)
        log.info(f"FASE FINAL: phase={bot.phase} answer_sent={bot.answer_sent} dc_opened={bot.dc_opened}")
        log.info("=" * 70)
        bot.close()


if __name__ == "__main__":
    asyncio.run(main_async())
