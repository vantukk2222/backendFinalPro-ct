// ✅ Simple Socket.IO server for real-time voice translation (no DB)

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
  },
});

const userSockets = new Map();

io.on('connection', (socket) => {
  console.log('🔌 New client connected:', socket.id);

  socket.on('register', (userId) => {
    userSockets.set(userId, socket.id);
    console.log(`👤 Registered userId ${userId} -> socket ${socket.id}`);
  });

  socket.on('send_translation', ({ toUserId, fromUserId, text, lang, isFinal  }) => {
    const toSocketId = userSockets.get(toUserId);
    if (toSocketId) {
      io.to(toSocketId).emit('receive_translation', {
        fromUserId,
        text,
        lang,
        isFinal 
      });
      console.log(`➡️ ${fromUserId} → ${toUserId}: ${text} (${lang})`);
      console.log(`isFinal: ${isFinal}`);
    }
  });

  // Ngắt kết nối
  socket.on('disconnect', () => {
    for (const [userId, sId] of userSockets.entries()) {
      if (sId === socket.id) {
        userSockets.delete(userId);
        console.log(`❌ Disconnected ${userId} (${socket.id})`);
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Socket server running on port ${PORT}`);
});