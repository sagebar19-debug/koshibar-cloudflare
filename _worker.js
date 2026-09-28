// Configuration de ton serveur VLESS
const userID = 'e1a49c7e-df1a-45b5-b218-8679d031e80b'; // Ton UUID
const proxyIP = '104.16.132.229'; // IP Clean Cloudflare

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      const url = new URL(request.url);

      // Traitement des requêtes WebSocket sur le path /koshibar
      if (upgradeHeader === 'websocket' && url.pathname.startsWith('/koshibar')) {
        return await vlessOverWSHandler(request);
      }

      return new Response(JSON.stringify({ status: "Active", path: "/koshibar" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);

  server.accept();

  let remoteSocketWrapper = { value: null };
  let log = (...args) => console.log(`[VLESS]`, ...args);

  const readableWebSocketStream = makeReadableWebSocketStream(server, log);

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (remoteSocketWrapper.value) {
        const writer = remoteSocketWrapper.value.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      if (chunk.byteLength < 18) return;

      const clientID = stringifyUUID(new Uint8Array(chunk.slice(1, 17)));
      if (clientID !== userID.toLowerCase()) {
        console.error("UUID invalide :", clientID);
        return;
      }

      const version = new Uint8Array(chunk.slice(0, 1))[0];
      const optLength = new Uint8Array(chunk.slice(17, 18))[0];
      const command = new Uint8Array(chunk.slice(18 + optLength, 19 + optLength))[0];

      if (command !== 1) {
        console.error("Commande non supportée :", command);
        return;
      }

      let offset = 19 + optLength;
      const portBuffer = chunk.slice(offset, offset + 2);
      const port = new DataView(portBuffer).getUint16(0);
      offset += 2;

      const addressType = new Uint8Array(chunk.slice(offset, offset + 1))[0];
      offset += 1;

      let address = '';
      if (addressType === 1) {
        address = new Uint8Array(chunk.slice(offset, offset + 4)).join('.');
        offset += 4;
      } else if (addressType === 2) {
        const domainLen = new Uint8Array(chunk.slice(offset, offset + 1))[0];
        offset += 1;
        address = new TextDecoder().decode(chunk.slice(offset, offset + domainLen));
        offset += domainLen;
      } else if (addressType === 3) {
        address = Array.from(new Uint16Array(chunk.slice(offset, offset + 16)))
          .map(n => n.toString(16)).join(':');
        offset += 16;
      }

      const rawClientData = chunk.slice(offset);

      server.send(new Uint8Array([version, 0]));

      try {
        const tcpSocket = connect({
          hostname: address,
          port: port,
        });

        remoteSocketWrapper.value = tcpSocket;

        if (rawClientData.byteLength > 0) {
          const writer = tcpSocket.writable.getWriter();
          await writer.write(rawClientData);
          writer.releaseLock();
        }

        tcpSocket.readable.pipeTo(new WritableStream({
          async write(remoteChunk) {
            server.send(remoteChunk);
          },
          close() {
            server.close();
          },
          abort(reason) {
            server.close();
          }
        })).catch(() => server.close());

      } catch (err) {
        console.error("Erreur de connexion socket :", err);
      }
    },
    close() {
      if (remoteSocketWrapper.value) remoteSocketWrapper.value.close();
    }
  })).catch(err => console.error("Erreur du flux WS :", err));

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

function makeReadableWebSocketStream(webSocket, log) {
  return new ReadableStream({
    start(controller) {
      webSocket.addEventListener('message', (event) => {
        controller.enqueue(event.data);
      });
      webSocket.addEventListener('close', () => {
        controller.close();
      });
      webSocket.addEventListener('error', (err) => {
        controller.error(err);
      });
    }
  });
}

function stringifyUUID(bytes) {
  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0'));
  return [
    hex.slice(0, 4).join(''),
    hex.slice(4, 6).join(''),
    hex.slice(6, 8).join(''),
    hex.slice(8, 10).join(''),
    hex.slice(10, 16).join('')
  ].join('-');
}
