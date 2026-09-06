import "dotenv/config";

import cors from "cors";
import express, { type Request, type Response } from "express";
import { createServer } from "http";
import { randomUUID } from "crypto";
import { Server, type Socket } from "socket.io";

type SocketMode = "idle" | "searching" | "chatting";

type PresenceSnapshot = {
  onlineCount: number;
  searchingCount: number;
  activeMatches: number;
};

type MatchPayload = {
  roomId: string;
  initiatorId: string;
  partnerId: string;
};

type OfferPayload = {
  roomId: string;
  sdp: RTCSessionDescriptionInit;
};

type AnswerPayload = {
  roomId: string;
  sdp: RTCSessionDescriptionInit;
};

type IcePayload = {
  roomId: string;
  candidate: RTCIceCandidateInit;
};

type ChatPayload = {
  roomId: string;
  id: string;
  senderId: string;
  text: string;
  timestamp: string;
};

type ServerToClientEvents = {
  "presence:update": (snapshot: PresenceSnapshot) => void;
  "match:found": (payload: MatchPayload) => void;
  "webrtc:offer": (payload: OfferPayload & { from: string }) => void;
  "webrtc:answer": (payload: AnswerPayload & { from: string }) => void;
  "webrtc:ice-candidate": (payload: IcePayload & { from: string }) => void;
  "chat:message": (payload: ChatPayload) => void;
  "partner:left": (payload: { reason: string; from: string }) => void;
  "queue:status": (payload: { status: string; message: string }) => void;
};

type ClientToServerEvents = {
  "queue:join": () => void;
  "queue:leave": () => void;
  "session:next": () => void;
  "session:stop": () => void;
  "webrtc:offer": (payload: OfferPayload) => void;
  "webrtc:answer": (payload: AnswerPayload) => void;
  "webrtc:ice-candidate": (payload: IcePayload) => void;
  "chat:message": (payload: ChatPayload) => void;
};

type ChatSocket = Socket<ClientToServerEvents, ServerToClientEvents>;

type ClientState = {
  mode: SocketMode;
  roomId: string | null;
  partnerId: string | null;
  cooldownUntil: number;
};

type ActiveSession = {
  roomId: string;
  members: [string, string];
  createdAt: number;
};

const PORT = Number(process.env.PORT ?? 4001);
const app = express();
const httpServer = createServer(app);

app.use(cors());
app.use(express.json());

const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

const clientStates = new Map<string, ClientState>();
const waitingQueue: string[] = [];
const activeSessions = new Map<string, ActiveSession>();

function getState(socketId: string): ClientState {
  const existing = clientStates.get(socketId);
  if (existing) {
    return existing;
  }

  const nextState: ClientState = {
    mode: "idle",
    roomId: null,
    partnerId: null,
    cooldownUntil: 0,
  };
  clientStates.set(socketId, nextState);
  return nextState;
}

function updateState(socketId: string, patch: Partial<ClientState>) {
  const current = getState(socketId);
  clientStates.set(socketId, {
    ...current,
    ...patch,
  });
}

function removeFromQueue(socketId: string) {
  const index = waitingQueue.indexOf(socketId);
  if (index !== -1) {
    waitingQueue.splice(index, 1);
  }
}

function queueSocket(socket: ChatSocket, message = "Searching for a stranger...") {
  const socketId = socket.id;
  removeFromQueue(socketId);
  updateState(socketId, {
    mode: "searching",
    roomId: null,
    partnerId: null,
  });
  waitingQueue.push(socketId);
  socket.emit("queue:status", {
    status: "waiting",
    message,
  });
}

function presenceSnapshot(): PresenceSnapshot {
  const onlineCount = io.engine.clientsCount;
  const searchingCount = [...clientStates.values()].filter((state) => state.mode === "searching").length;

  return {
    onlineCount,
    searchingCount,
    activeMatches: activeSessions.size,
  };
}

function emitPresence() {
  io.emit("presence:update", presenceSnapshot());
}

function isMatchable(socketId: string) {
  const socket = io.sockets.sockets.get(socketId);
  const state = clientStates.get(socketId);
  return Boolean(socket && state && state.mode === "searching" && state.cooldownUntil <= Date.now());
}

function leaveRoom(socketId: string, roomId: string) {
  const socket = io.sockets.sockets.get(socketId);
  socket?.leave(roomId);
}

function requeueAfterPartnerLeaves(socketId: string) {
  const socket = io.sockets.sockets.get(socketId);
  if (!socket) {
    return;
  }

  updateState(socketId, {
    mode: "searching",
    roomId: null,
    partnerId: null,
    cooldownUntil: Date.now() + 2000,
  });

  queueSocket(socket, "Your stranger left. Searching for a new match...");
  emitPresence();
}

function endSession(
  socketId: string,
  options: {
    reason: string;
    requeueSelf: boolean;
    requeuePartner: boolean;
    message?: string;
  },
) {
  const state = clientStates.get(socketId);
  if (!state?.roomId) {
    if (options.requeueSelf) {
      const socket = io.sockets.sockets.get(socketId);
      if (socket) {
        queueSocket(socket, options.message ?? "Searching for a stranger...");
        emitPresence();
      }
    } else {
      updateState(socketId, {
        mode: "idle",
        roomId: null,
        partnerId: null,
      });
      removeFromQueue(socketId);
      emitPresence();
    }
    return;
  }

  const { roomId, partnerId } = state;
  const session = activeSessions.get(roomId);
  activeSessions.delete(roomId);

  leaveRoom(socketId, roomId);
  updateState(socketId, {
    mode: options.requeueSelf ? "searching" : "idle",
    roomId: null,
    partnerId: null,
    cooldownUntil: options.requeueSelf ? Date.now() + 2000 : 0,
  });

  if (options.requeueSelf) {
    const socket = io.sockets.sockets.get(socketId);
    if (socket) {
      queueSocket(socket, options.message ?? "Searching for a stranger...");
    }
  } else {
    removeFromQueue(socketId);
  }

  if (partnerId) {
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (partnerSocket) {
      leaveRoom(partnerId, roomId);
      partnerSocket.emit("partner:left", {
        reason: options.reason,
        from: socketId,
      });
    }

    if (options.requeuePartner) {
      updateState(partnerId, {
        mode: "searching",
        roomId: null,
        partnerId: null,
        cooldownUntil: Date.now() + 2000,
      });
      if (partnerSocket) {
        queueSocket(partnerSocket, "Your stranger left. Searching for a new match...");
      }
    } else {
      updateState(partnerId, {
        mode: "idle",
        roomId: null,
        partnerId: null,
        cooldownUntil: 0,
      });
      removeFromQueue(partnerId);
    }
  }

  emitPresence();
}

function matchSockets(aId: string, bId: string) {
  const socketA = io.sockets.sockets.get(aId);
  const socketB = io.sockets.sockets.get(bId);

  if (!socketA || !socketB) {
    removeFromQueue(aId);
    removeFromQueue(bId);
    return;
  }

  const roomId = `match:${randomUUID()}`;
  const initiatorId = Math.random() < 0.5 ? aId : bId;
  const partnerIdA = bId;
  const partnerIdB = aId;

  removeFromQueue(aId);
  removeFromQueue(bId);

  updateState(aId, {
    mode: "chatting",
    roomId,
    partnerId: partnerIdA,
    cooldownUntil: 0,
  });
  updateState(bId, {
    mode: "chatting",
    roomId,
    partnerId: partnerIdB,
    cooldownUntil: 0,
  });

  socketA.join(roomId);
  socketB.join(roomId);
  activeSessions.set(roomId, {
    roomId,
    members: [aId, bId],
    createdAt: Date.now(),
  });

  const payloadA: MatchPayload = {
    roomId,
    initiatorId,
    partnerId: partnerIdA,
  };
  const payloadB: MatchPayload = {
    roomId,
    initiatorId,
    partnerId: partnerIdB,
  };

  socketA.emit("match:found", payloadA);
  socketB.emit("match:found", payloadB);
  socketA.emit("queue:status", {
    status: "matched",
    message: "A stranger was found. Say hello!",
  });
  socketB.emit("queue:status", {
    status: "matched",
    message: "A stranger was found. Say hello!",
  });

  emitPresence();
}

function attemptMatch() {
  const available = waitingQueue.filter((socketId) => isMatchable(socketId));
  if (available.length < 2) {
    return;
  }

  for (let i = 0; i < available.length; i += 1) {
    for (let j = i + 1; j < available.length; j += 1) {
      const aId = available[i];
      const bId = available[j];
      if (aId === bId) {
        continue;
      }

      matchSockets(aId, bId);
      return attemptMatch();
    }
  }
}

function relayOffer(socket: ChatSocket, payload: OfferPayload) {
  const state = clientStates.get(socket.id);
  if (!state?.roomId) {
    return;
  }

  socket.to(state.roomId).emit("webrtc:offer", {
    ...payload,
    from: socket.id,
  });
}

function relayAnswer(socket: ChatSocket, payload: AnswerPayload) {
  const state = clientStates.get(socket.id);
  if (!state?.roomId) {
    return;
  }

  socket.to(state.roomId).emit("webrtc:answer", {
    ...payload,
    from: socket.id,
  });
}

function relayIceCandidate(socket: ChatSocket, payload: IcePayload) {
  const state = clientStates.get(socket.id);
  if (!state?.roomId) {
    return;
  }

  socket.to(state.roomId).emit("webrtc:ice-candidate", {
    ...payload,
    from: socket.id,
  });
}

function relayChatMessage(socket: ChatSocket, payload: ChatPayload) {
  const state = clientStates.get(socket.id);
  if (!state?.roomId) {
    return;
  }

  socket.to(state.roomId).emit("chat:message", {
    ...payload,
    senderId: socket.id,
  });
}

io.on("connection", (socket: ChatSocket) => {
  clientStates.set(socket.id, {
    mode: "idle",
    roomId: null,
    partnerId: null,
    cooldownUntil: 0,
  });
  emitPresence();

  socket.emit("presence:update", presenceSnapshot());

  socket.on("queue:join", () => {
    queueSocket(socket);
    emitPresence();
    attemptMatch();
  });

  socket.on("queue:leave", () => {
    removeFromQueue(socket.id);
    updateState(socket.id, {
      mode: "idle",
      roomId: null,
      partnerId: null,
      cooldownUntil: 0,
    });
    emitPresence();
  });

  socket.on("session:next", () => {
    endSession(socket.id, {
      reason: "next",
      requeueSelf: true,
      requeuePartner: true,
      message: "Searching for the next stranger...",
    });
    attemptMatch();
  });

  socket.on("session:stop", () => {
    endSession(socket.id, {
      reason: "stop",
      requeueSelf: false,
      requeuePartner: true,
    });
    attemptMatch();
  });

  socket.on("webrtc:offer", (payload: OfferPayload) => {
    relayOffer(socket, payload);
  });

  socket.on("webrtc:answer", (payload: AnswerPayload) => {
    relayAnswer(socket, payload);
  });

  socket.on("webrtc:ice-candidate", (payload: IcePayload) => {
    relayIceCandidate(socket, payload);
  });

  socket.on("chat:message", (payload: ChatPayload) => {
    relayChatMessage(socket, payload);
  });

  socket.on("disconnect", () => {
    const state = clientStates.get(socket.id);
    removeFromQueue(socket.id);
    clientStates.delete(socket.id);

    if (state?.roomId) {
      const session = activeSessions.get(state.roomId);
      if (session) {
        activeSessions.delete(state.roomId);
      }

      const partnerId = state.partnerId;
      if (partnerId) {
        const partnerSocket = io.sockets.sockets.get(partnerId);
        partnerSocket?.emit("partner:left", {
          reason: "disconnect",
          from: socket.id,
        });
        if (partnerSocket) {
          requeueAfterPartnerLeaves(partnerId);
        }
      }
    }

    emitPresence();
    attemptMatch();
  });
});

app.get("/health", (_request: Request, response: Response) => {
  response.json({
    ok: true,
    service: "chatwave-signaling-server",
    onlineCount: presenceSnapshot().onlineCount,
    searchingCount: presenceSnapshot().searchingCount,
    activeMatches: presenceSnapshot().activeMatches,
  });
});

app.get("/", (_request: Request, response: Response) => {
  response.json({
    ok: true,
    message: "ChatWave signaling server is running.",
  });
});

setInterval(() => {
  attemptMatch();
  emitPresence();
}, 1000).unref();

httpServer.listen(PORT | 4001, () => {
  console.log(`ChatWave signaling server listening on port ${PORT}`);
});
