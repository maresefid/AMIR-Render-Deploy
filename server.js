import http from "node:http";
import net from "node:net";
import tls from "node:tls";
import { Readable } from "node:stream";
import { WebSocketServer, WebSocket } from "ws";

const nativeResponse = globalThis.Response;

class PairSocket extends EventTarget {
  constructor() {
    super();
    this.readyState = WebSocket.OPEN;
    this.binaryType = "arraybuffer";
    this.peer = null;
  }
  accept() {}
  send(data) {
    if (this.readyState !== WebSocket.OPEN || !this.peer) return;
    const payload = data instanceof ArrayBuffer
      ? data.slice(0)
      : ArrayBuffer.isView(data)
        ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
        : data;
    queueMicrotask(() => {
      if (this.peer?.readyState === WebSocket.OPEN) {
        this.peer.dispatchEvent(new MessageEvent("message", { data: payload }));
      }
    });
  }
  close(code = 1000, reason = "") {
    if (this.readyState !== WebSocket.OPEN) return;
    this.readyState = WebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
    if (this.peer && this.peer.readyState !== WebSocket.CLOSED) {
      this.peer.readyState = WebSocket.CLOSED;
      queueMicrotask(() => this.peer.dispatchEvent(new Event("close")));
    }
  }
  addEventListener(type, listener, options) {
    return super.addEventListener(type, listener, options);
  }
}
globalThis.WebSocketPair = class WebSocketPair {
  constructor() {
    const a = new PairSocket(), b = new PairSocket();
    a.peer = b; b.peer = a;
    this[0] = a; this[1] = b;
  }
};

// Cloudflare Workers permit a 101 Response with a webSocket field. Node's
// built-in Response rejects status 101, so represent only that special result
// with a marker object; ordinary responses remain native Response instances.
globalThis.Response = class RenderResponse extends nativeResponse {
  constructor(body, init = {}) {
    if (init.status === 101 && init.webSocket) {
      return {
        __renderWebSocketUpgrade: true,
        status: 101,
        headers: new Headers(init.headers || {}),
        webSocket: init.webSocket
      };
    }
    super(body, init);
  }
};

function makeTcpSocket(options, init = {}) {
  const hostname = String(options?.hostname || options?.host || "");
  const port = Number(options?.port);
  if (!hostname || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("Invalid TCP destination");
  }
  const secure = init.secureTransport === "on";
  const socket = secure
    ? tls.connect({ host: hostname.replace(/^\[|\]$/g, ""), port, rejectUnauthorized: true })
    : net.connect({ host: hostname.replace(/^\[|\]$/g, ""), port });

  let resolveOpened, rejectOpened, resolveClosed;
  const opened = new Promise((resolve, reject) => { resolveOpened = resolve; rejectOpened = reject; });
  const closed = new Promise(resolve => { resolveClosed = resolve; });
  closed.catch(() => {});
  socket.once(secure ? "secureConnect" : "connect", resolveOpened);
  socket.once("error", err => { rejectOpened(err); });
  socket.once("close", () => resolveClosed());

  const readable = new ReadableStream({
    start(controller) {
      socket.on("data", chunk => {
        try { controller.enqueue(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)); }
        catch {}
      });
      socket.once("end", () => { try { controller.close(); } catch {} });
      socket.on("error", err => { try { controller.error(err); } catch {} });
      socket.once("close", () => { try { controller.close(); } catch {} });
    },
    cancel() { socket.destroy(); }
  });

  const writable = new WritableStream({
    async write(chunk) {
      if (socket.destroyed || !socket.writable) throw new Error("TCP socket is not writable");
      const buf = Buffer.from(chunk instanceof ArrayBuffer ? new Uint8Array(chunk) : chunk);
      await new Promise((resolve, reject) => {
        const onError = err => { cleanup(); reject(err); };
        const onDrain = () => { cleanup(); resolve(); };
        const cleanup = () => { socket.off("error", onError); socket.off("drain", onDrain); };
        socket.once("error", onError);
        if (socket.write(buf)) { cleanup(); resolve(); }
        else socket.once("drain", onDrain);
      });
    },
    close() { socket.end(); },
    abort() { socket.destroy(); }
  });

  return {
    opened, closed, readable, writable,
    close() { socket.destroy(); },
    startTls() { throw new Error("startTls is not implemented in this Render adapter"); }
  };
}

async function createKV() {
  if (process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN) {
    const { Redis } = await import("@upstash/redis");
    const redis = new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN
    });
    return {
      get: async key => {
        const value = await redis.get(key);
        return value == null ? null : (typeof value === "string" ? value : JSON.stringify(value));
      },
      put: async (key, value) => { await redis.set(key, String(value)); },
      delete: async key => { await redis.del(key); }
    };
  }
  if (process.env.REDIS_URL) {
    const { default: Redis } = await import("ioredis");
    const redis = new Redis(process.env.REDIS_URL, {
      lazyConnect: true,
      maxRetriesPerRequest: 2,
      enableReadyCheck: true
    });
    return {
      get: async key => {
        if (redis.status === "wait") await redis.connect();
        return redis.get(key);
      },
      put: async (key, value) => {
        if (redis.status === "wait") await redis.connect();
        await redis.set(key, String(value));
      },
      delete: async key => {
        if (redis.status === "wait") await redis.connect();
        await redis.del(key);
      }
    };
  }
  // Development fallback only. Render's filesystem and this Map are ephemeral.
  const map = new Map();
  console.warn("WARNING: no persistent KV configured; in-memory data will be lost on restart/sleep.");
  return {
    async get(key) { return map.has(key) ? map.get(key) : null; },
    async put(key, value) { map.set(key, String(value)); },
    async delete(key) { map.delete(key); }
  };
}

function bridgeExternalWebSocket(external, workerSide) {
  workerSide.addEventListener("message", event => {
    if (external.readyState === WebSocket.OPEN) {
      const data = event.data;
      if (data instanceof ArrayBuffer) external.send(Buffer.from(data));
      else if (ArrayBuffer.isView(data)) external.send(Buffer.from(data.buffer, data.byteOffset, data.byteLength));
      else external.send(data);
    }
  });
  workerSide.addEventListener("close", () => {
    if (external.readyState === WebSocket.OPEN || external.readyState === WebSocket.CONNECTING) {
      try { external.close(); } catch {}
    }
  });
  workerSide.addEventListener("error", () => {
    try { external.close(1011, "worker websocket error"); } catch {}
  });

  external.on("message", (data, isBinary) => {
    if (workerSide.readyState !== WebSocket.CLOSED) {
      const payload = isBinary
        ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
        : data.toString();
      workerSide.dispatchEvent(new MessageEvent("message", { data: payload }));
    }
  });
  external.on("close", () => {
    if (workerSide.readyState !== WebSocket.CLOSED) {
      workerSide.readyState = WebSocket.CLOSED;
      workerSide.dispatchEvent(new Event("close"));
    }
  });
  external.on("error", err => {
    workerSide.dispatchEvent(new ErrorEvent("error", { error: err, message: err.message }));
  });
}

function sendResponse(res, result) {
  if (result?.__renderWebSocketUpgrade) {
    res.writeHead(426, { "Content-Type": "text/plain; charset=utf-8", "Upgrade": "websocket" });
    res.end("This endpoint requires a WebSocket upgrade.");
    return;
  }
  res.statusCode = result?.status || 500;
  for (const [key, value] of result?.headers || []) res.setHeader(key, value);
  if (!result?.body) { res.end(); return; }
  Readable.fromWeb(result.body).on("error", () => res.destroy()).pipe(res);
}

const env = {
  ...process.env,
  ADMIN: process.env.ADMIN || process.env.ADMIN_PASSWORD || process.env.PASSWORD,
  KEY: process.env.KEY || process.env.SESSION_SECRET,
  UUID: process.env.UUID || process.env.uuid,
  PAGES_URL: process.env.PAGES_URL || "",
  BEST_SUB: process.env.BEST_SUB || "0",
  DEBUG: process.env.DEBUG || "0"
};
env.KV = await createKV();

const { default: worker } = await import("./worker_AMIR.js");
const server = http.createServer(async (incoming, outgoing) => {
  try {
    const host = incoming.headers.host || "localhost";
    const url = `http://${host}${incoming.url || "/"}`;
    const headers = new Headers();
    for (const [key, value] of Object.entries(incoming.headers)) {
      if (Array.isArray(value)) value.forEach(v => headers.append(key, v));
      else if (value !== undefined) headers.set(key, String(value));
    }
    let body;
    if (!["GET", "HEAD"].includes(incoming.method || "GET")) {
      const chunks = [];
      for await (const chunk of incoming) chunks.push(chunk);
      body = Buffer.concat(chunks);
    }
    const request = new Request(url, {
      method: incoming.method,
      headers,
      ...(body ? { body, duplex: "half" } : {})
    });
    Object.defineProperty(request, "cf", {
      value: {
        colo: "RENDER",
        asn: 0,
        asOrganization: "Render",
        country: incoming.headers["cf-ipcountry"] || "XX",
        city: "",
        httpProtocol: incoming.httpVersion ? `HTTP/${incoming.httpVersion}` : "HTTP/1.1",
        tlsVersion: "unknown"
      }
    });
    Object.defineProperty(request, "fetcher", { value: { connect: makeTcpSocket } });
    const result = await worker.fetch(request, env, { waitUntil() {} });
    sendResponse(outgoing, result);
  } catch (error) {
    console.error("Request failed:", error?.stack || error);
    if (!outgoing.headersSent) outgoing.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
    outgoing.end("Internal Server Error");
  }
});

const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 16 * 1024 * 1024 });
server.on("upgrade", (incoming, socket, head) => {
  wss.handleUpgrade(incoming, socket, head, async external => {
    try {
      const host = incoming.headers.host || "localhost";
      const headers = new Headers();
      for (const [key, value] of Object.entries(incoming.headers)) {
        if (Array.isArray(value)) value.forEach(v => headers.append(key, v));
        else if (value !== undefined) headers.set(key, String(value));
      }
      const request = new Request(`http://${host}${incoming.url || "/"}`, { method: "GET", headers });
      Object.defineProperty(request, "cf", { value: { colo: "RENDER", asn: 0, asOrganization: "Render", country: incoming.headers["cf-ipcountry"] || "XX", city: "", httpProtocol: "HTTP/1.1", tlsVersion: "unknown" } });
      Object.defineProperty(request, "fetcher", { value: { connect: makeTcpSocket } });
      const result = await worker.fetch(request, env, { waitUntil() {} });
      if (result?.__renderWebSocketUpgrade && result.webSocket) {
        bridgeExternalWebSocket(external, result.webSocket);
      } else {
        try { external.close(1008, "WebSocket route not accepted"); } catch {}
      }
    } catch (error) {
      console.error("WebSocket upgrade failed:", error?.stack || error);
      try { external.close(1011, "internal error"); } catch {}
    }
  });
});

const port = Number(process.env.PORT || 10000);
server.listen(port, "0.0.0.0", () => console.log(`AMIR Edge Panel listening on 0.0.0.0:${port}`));
