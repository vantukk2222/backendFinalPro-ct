// server.js
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const admin = require('firebase-admin');
// const serviceAccount = require('./server/firebase-service-account.json'); // Đường dẫn tới file JSON service account
require('dotenv').config();
const serviceAccount = {
  type: process.env.FIREBASE_TYPE,
  project_id: process.env.FIREBASE_PROJECT_ID,
  private_key_id: process.env.FIREBASE_PRIVATE_KEY_ID,
  private_key: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  client_email: process.env.FIREBASE_CLIENT_EMAIL,
  client_id: process.env.FIREBASE_CLIENT_ID,
  auth_uri: process.env.FIREBASE_AUTH_URI,
  token_uri: process.env.FIREBASE_TOKEN_URI,
  auth_provider_x509_cert_url: process.env.FIREBASE_AUTH_PROVIDER_X509_CERT_URL,
  client_x509_cert_url: process.env.FIREBASE_CLIENT_X509_CERT_URL,
  universe_domain: process.env.FIREBASE_UNIVERSE_DOMAIN,
};

const app = express();
app.use(cors());

// Khởi tạo Firebase Admin SDK
admin.initializeApp({

  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
});

// Map lưu socketId theo userId
const userSockets = new Map();
// Map lưu token FCM theo userId
const userTokens = new Map();

/**
 * Lấy token FCM từ cache (userTokens) hoặc Firestore nếu chưa có trong cache
 * @param {string} userId 
 * @returns {Promise<string|null>} fcmToken hoặc null nếu không tìm thấy
 */
async function getFcmToken(userId) {
  if (userTokens.has(userId)) {
    return userTokens.get(userId);
  }
  try {
    console.log(`Fetching FCM token for user ${userId} from Firestore...`);
    const userDoc = await db.collection('users').doc(userId).get();
    if (userDoc.exists) {
      // console.log("userDoc.data()", userDoc.data());
      const token = userDoc.data().fcmToken;
      if (token) {
        userTokens.set(userId, token); // Cache lại token
        return token;
      }
    }
  } catch (error) {
    console.error(`Error fetching FCM token for user ${userId}:`, error);
  }
  return null;
}

/**
 * Gửi thông báo đẩy qua FCM
 * @param {string} token 
 * @param {string} title 
 * @param {string} body 
 * @param {Object} data 
 */
async function sendPushNotification(token, title, body, data = {}) {
 const message = {
    notification: { title, body },
    data,
    token,
  };

  try {
    const response = await admin.messaging().send(message);
    console.log('Successfully sent message:', response);
  } catch (error) {
    console.error('Error sending message:', error);
  }
}

io.on('connection', (socket) => {
  console.log('🔌 New client connected:', socket.id);

  // Khi client đăng ký userId và token FCM
  socket.on('register', async ({ userId, fcmToken,from }) => {
    console.log("from: ", from);
    userSockets.set(userId, socket.id);
    if (fcmToken) {
      userTokens.set(userId, fcmToken);
      console.log(`👤 Registered userId ${userId} with FCM token from client`);
      // Cập nhật token mới lên Firestore
      try {
        await db.collection('users').doc(userId).update({ fcmToken });
      } catch (error) {
        console.error(`Failed to update FCM token for user ${userId}:`, error);
      }
    } else {
      // Nếu client không gửi token, cố gắng lấy token từ Firestore
      const token = await getFcmToken(userId);
      if (token) {
        console.log(`👤 Registered userId ${userId} with FCM token from Firestore cache`);
      } else {
        console.log(`⚠️ User ${userId} không có token FCM`);
      }
    }
  });

  // Nhận sự kiện dịch thuật, gửi realtime tới user đích
  socket.on('send_translation', ({ toUserId, fromUserId, text, lang, isFinal }) => {
    const toSocketId = userSockets.get(toUserId);
    if (toSocketId) {
      io.to(toSocketId).emit('receive_translation', { fromUserId, text, lang, isFinal });
      console.log(`➡️ ${fromUserId} → ${toUserId}: ${text} (${lang}) isFinal: ${isFinal}`);
    }
  });
  socket.on('start_call', async ({ meetingId, fromUserId, memberIds }) => {
    console.log('start_call', { meetingId, fromUserId, memberIds });
    if (!Array.isArray(memberIds)) {
      console.warn('start_call memberIds is not array:', memberIds);
      return;
    }

    // Lấy thông tin cuộc gọi từ Firestore
    const meetingRef = db.collection('meetings').doc(meetingId);
    const meetingDoc = await meetingRef.get();

    if (!meetingDoc.exists) {
      console.warn('Meeting not found:', meetingId);
      return;
    }

    const currentMembers = meetingDoc.data()?.members || [];
    console.log('Current members in meeting:', currentMembers);

    // Lặp qua tất cả memberIds và gửi thông báo cho những người chưa tham gia
    for (const memberId of memberIds) {
      if (!memberId) {
        console.warn('start_call found undefined memberId, skipping');
        continue;
      }

      if (memberId !== fromUserId) {
        // Kiểm tra nếu thành viên đã tham gia cuộc gọi
        const alreadyInCall = currentMembers.some(member => member.uid === memberId);

        if (!alreadyInCall) {
          console.log(`📞 Sending push notification to ${memberId} about new group call`);
          
          // Lấy token FCM của người nhận
          const token = await getFcmToken(memberId);
          if (token) {
            // Gửi push notification
            await sendPushNotification(
              token,
              'Cuộc gọi nhóm mới',
              'Bạn có cuộc gọi nhóm, hãy tham gia ngay!',
              { meetingId }
            );
          } else {
            console.log(`⚠️ User ${memberId} không có token FCM`);
          }
        } else {
          console.log(`🟢 User ${memberId} đã tham gia cuộc gọi, không gửi thông báo`);
        }
      }
    }
  });


  // Xử lý ngắt kết nối
  socket.on('disconnect', () => {
    for (const [userId, sId] of userSockets.entries()) {
      if (sId === socket.id) {
        userSockets.delete(userId);
        userTokens.delete(userId);
        console.log(`❌ Disconnected ${userId} (${socket.id})`);
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 3001;
app.get('/api/example', (req, res) => {
  res.json({ message: 'Hello from the example API route!' });
});

server.listen(PORT, () => {
  console.log(`🚀 Socket server running on port ${PORT}`);
});
