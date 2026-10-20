#!/usr/bin/env python3
"""Answerer ToDus con reactor-webrtc. Descarga canción al arrancar y la transmite por audio.

Diferencias con aiortc:
  - API asíncrona nativa (await create_offer, await set_local_description).
  - RtcConfiguration.ice_transport_type="relay" fuerza solo candidatos TURN.
  - El audio se empuja con factory.push_audio_frame(pcm_bytes, ...) en un loop.
"""
import os, re, ssl, base64, hashlib, socket, threading, time, uuid, json, asyncio, logging, sys
import requests, urllib3
import av
import reactor_webrtc as rw

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PHONE       = os.environ.get("TODUS_PHONE", "5351417372")
AUTH_URL    = "https://auth.todus.cu/v2/auth/token"
XMPP_HOST   = os.environ.get("XMPP_HOST", "ws.todus.cu")
XMPP_PORT   = int(os.environ.get("XMPP_PORT", "1756"))
XMPP_DOMAIN = "im.todus.cu"
FAKE_SECRET = "00000000-0000-0000-0000-000000000000"
RECV_BUFFER = 65536
RUNTIME     = int(os.environ.get("RUNTIME", "300"))
AUDIO_URL   = os.environ.get("AUDIO_URL", "")
AUDIO_FILE  = os.environ.get("AUDIO_FILE", "cancion.mp3")
VPS         = os.environ.get("VPS", "false").lower() == "true"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("rw")


def descargar_cancion():
    if os.path.exists(AUDIO_FILE) and os.path.getsize(AUDIO_FILE) > 1024:
        log.info(f"*** Canción ya existe: {AUDIO_FILE} ({os.path.getsize(AUDIO_FILE)} bytes)")
        return True
    if not AUDIO_URL:
        log.warning("*** AUDIO_URL vacío")
        return False
    log.info(f"*** Descargando canción...")
    try:
        r = requests.get(AUDIO_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=60, stream=True, verify=False)
        r.raise_for_status()
        total = 0
        with open(AUDIO_FILE, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk); total += len(chunk)
        log.info(f"*** Canción descargada: {total} bytes")
        c = av.open(AUDIO_FILE)
        s = c.streams.audio[0]
        log.info(f"*** Canción válida: codec={s.codec_context.name} rate={s.rate}Hz ch={s.channels}")
        c.close()
        return True
    except Exception as e:
        log.warning(f"*** Error descargando: {e}")
        return False


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


class Answerer:
    def __init__(self, phone, has_audio):
        self.phone   = re.sub(r"[^\d]", "", phone)
        self.has_audio = has_audio
        self.jwt     = None
        self.sock    = None
        self.buf     = b""
        self.send_lock = threading.Lock()
        self.running = True
        self.full_jid = f"{self.phone}@{XMPP_DOMAIN}"
        self.peer_jid = None
        self.call_id  = None
        self.pc       = None
        self.answer_sent = False
        self._loop    = None
        self.phase    = "init"
        self._turn_iq_id = None
        self._turn_response = None
        self._turn_event = threading.Event()
        self._factory = None
        self._audio_task = None

    def send(self, data, label=""):
        with self.send_lock:
            raw = data.encode() if isinstance(data, str) else data
            log.info(f">>> [{label}] {raw[:300]!r}")
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
        resource = hashlib.md5(self.phone.encode()).hexdigest() + "_rw_" + uuid.uuid4().hex[:8]
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
        log.info(f"Esperando CALL en {self.phone} (audio={'sí' if self.has_audio else 'silencio'})")
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
        log.info(f"\n<<< MSG RAW: {stanza[:400]}")
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
            f'<b>PICKED</b></m>', "PICKED")
        log.info("*** PICKED enviado")

    def _on_iq(self, stanza):
        if self._turn_iq_id:
            m_id = re.search(r"\bi(?:d)?=['\"]([^'\"]+)['\"]", stanza)
            if m_id and m_id.group(1) == self._turn_iq_id and "todus:turn:cred" in stanza:
                self._handle_turn_response(stanza); return
        if "todus:call:status" not in stanza: return
        q = re.search(r"status=['\"]([^'\"]+)['\"]", stanza)
        if not q: return
        log.info(f"\n<<< IQ STATUS={q.group(1)}")
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
                log.warning("*** IQ END"); self.phase = "peer_ended_iq"
        except Exception as e:
            log.warning(f"parse IQ: {e}")

    def pedir_turn(self):
        self._turn_iq_id = uuid.uuid4().hex
        self._turn_response = None
        self._turn_event.clear()
        self.send(
            f'<iq type="get" id="{self._turn_iq_id}" to="{XMPP_DOMAIN}">'
            f'<query xmlns="todus:turn:cred"/></iq>', "GET-TURN")
        if not self._turn_event.wait(timeout=15):
            log.warning("*** TURN: timeout"); return None
        return self._turn_response

    def _handle_turn_response(self, stanza):
        m = re.search(r"<query[^>]*>([^<]+)</query>", stanza)
        if not m:
            log.warning("*** TURN: sin contenido"); self._turn_event.set(); return
        try:
            b64_content = m.group(1).strip()
            pad = "=" * (-len(b64_content) % 4)
            decoded = base64.b64decode(b64_content + pad).decode("utf-8")
            turn = json.loads(decoded)
            log.info(f"*** TURN obtenido: user={turn.get('username')} ttl={turn.get('ttl')}")
            self._turn_response = turn
        except Exception as e:
            log.warning(f"*** TURN: error parseando: {e}")
        finally:
            self._turn_event.set()

    async def _enviar_audio_loop(self, factory):
        """Loop que empuja frames PCM de la canción al factory de reactor-webrtc."""
        if not self.has_audio or not os.path.exists(AUDIO_FILE):
            log.warning("*** Sin canción, no se empuja audio")
            return
        log.info(f"*** Iniciando push de audio: {AUDIO_FILE}")
        container = av.open(AUDIO_FILE)
        stream = container.streams.audio[0]
        resampler = av.AudioResampler(format="s16", layout="mono", rate=48000)
        count = 0
        try:
            while self.running:
                try:
                    frame = next(container.decode(stream))
                except StopIteration:
                    container.seek(0)
                    continue
                for resampled in resampler.resample(frame):
                    pcm = resampled.to_ndarray().tobytes()
                    factory.push_audio_frame(pcm, sample_rate=48000, channels=1)
                    count += 1
                    if count % 50 == 0:
                        log.info(f"*** Audio frames empujados: {count}")
                    await asyncio.sleep(0.02)
        except Exception as e:
            log.warning(f"*** push_audio error: {e}")
        finally:
            container.close()

    async def _responder_offer(self, offer_sdp):
        try:
            mlines = re.findall(r"^m=(\w+)", offer_sdp, re.MULTILINE)
            ncands = len(re.findall(r"^a=candidate:", offer_sdp, re.MULTILINE))
            log.info(f"*** SDP [OFFER-REMOTE] contiene {ncands} candidates, m-lines: {mlines}")

            use_turn = "no" if VPS else "yes"
            ice_servers = []
            if use_turn == "yes":
                log.info("*** Pidiendo TURN a ToDus...")
                turn = self.pedir_turn()
                if turn and turn.get("uris"):
                    urls = [f"{u['uri']}?transport={u['proto']}" for u in turn["uris"]]
                    ice_servers = [rw.IceServer(urls=urls, username=turn.get("username"), password=turn.get("password"))]
                    log.info(f"*** TURN configurado: {urls}")

            log.info("*** Creando RtcConfiguration con ice_transport_type=relay")
            config = rw.RtcConfiguration(
                ice_servers=ice_servers,
                ice_transport_type="relay" if use_turn == "yes" else "all",
            )

            self._factory = rw.PeerConnectionFactory()
            obs = rw.PeerConnectionObserver()
            obs.on_ice_candidate = lambda c: log.info(f"*** ICE candidate: {getattr(c, 'candidate', c)}")
            obs.on_connection_state_change = lambda s: log.info(f"*** PC state: {s}")
            obs.on_track = lambda t: log.info(f"*** Remote track: {t.kind if hasattr(t, 'kind') else t}")

            self.pc = self._factory.create_peer_connection(config, obs)
            log.info("*** PeerConnection creado")

            # Aplicar OFFER remoto
            offer = rw.SessionDescription(kind="offer", sdp=offer_sdp)
            await self.pc.set_remote_description(offer)
            log.info("*** OFFER remoto aplicado")

            # Crear ANSWER
            answer = await self.pc.create_answer()
            await self.pc.set_local_description(answer)
            log.info("*** ANSWER creado")

            # Esperar gathering
            await asyncio.sleep(2)
            local_desc = getattr(self.pc, "local_description", None)
            if local_desc:
                ncands2 = len(re.findall(r"^a=candidate:", local_desc.sdp, re.MULTILINE))
                amlines = re.findall(r"^m=(\w+)", local_desc.sdp, re.MULTILINE)
                log.info(f"*** SDP [ANSWER-LOCAL] contiene {ncands2} candidates, m-lines: {amlines}")

            await self._enviar_answer(answer.sdp)

            # Arrancar loop de audio
            if self.has_audio:
                self._audio_task = asyncio.create_task(self._enviar_audio_loop(self._factory))

            self.phase = "answer_sent"
        except Exception as e:
            log.exception(f"responder_offer: {e}")

    async def _enviar_answer(self, sdp):
        state_b64 = b64(f"ANSWER {sdp}")
        content = json.dumps({
            "callId": self.call_id, "endTime": -1,
            "msgId": f"call_{uuid.uuid4().hex}", "startTime": -1,
            "state": state_b64, "userId": self.phone + "@im.todus.cu", "type": 32,
        })
        iq_id = uuid.uuid4().hex
        self.send(
            f'<iq type="set" id="{iq_id}" to="{self.peer_jid}" from="{self.full_jid}">'
            f'<query xmlns="todus:call:status" '
            f'to_user="{self.peer_jid}" from_user="{self.full_jid}" '
            f'status="ANSWER" content="{b64(content)}"/></iq>', "ANSWER")
        self.answer_sent = True
        log.info("*** ANSWER enviado")

    async def _apply_ice(self, payload):
        if "$" in payload and payload.count("$") >= 2:
            parts = payload.split("$", 2)
        else:
            parts = payload.split(" ", 2)
        if len(parts) != 3:
            log.warning(f"ICE mal formado: {payload[:100]!r}"); return
        mid, mline, cand = parts
        try:
            if not mline.isdigit():
                log.warning(f"ICE mline no numérico: {mline!r}"); return
            # reactor-webrtc espera IceCandidate con sdp_mid, sdp_mline_index, candidate
            ice = rw.IceCandidate(sdp_mid=mid, sdp_mline_index=int(mline), candidate=cand)
            if self.pc:
                await self.pc.add_ice_candidate(ice)
                log.info(f"*** ICE remote aplicado (mline={mline})")
        except Exception as e:
            log.warning(f"ICE remote: {e}")

    def close(self):
        self.running = False
        try:
            if self._audio_task: self._audio_task.cancel()
        except Exception: pass
        try:
            if self.pc: self.pc.close()
        except Exception: pass
        try:
            if self.sock:
                self.send("</stream:stream>", "CLOSE"); self.sock.close()
        except Exception: pass


async def main_async():
    has_audio = descargar_cancion()
    loop = asyncio.get_running_loop()
    bot = Answerer(PHONE, has_audio)
    bot._loop = loop
    bot.conectar()
    threading.Thread(target=bot.reader, daemon=True).start()
    threading.Thread(target=bot.keepalive, daemon=True).start()
    log.info(f"LISTO - escuchando en {PHONE} por {RUNTIME}s")
    try:
        await asyncio.sleep(RUNTIME)
    finally:
        log.info("=" * 70)
        log.info(f"FASE FINAL: phase={bot.phase} answer_sent={bot.answer_sent}")
        log.info("=" * 70)
        bot.close()


if __name__ == "__main__":
    asyncio.run(main_async())
