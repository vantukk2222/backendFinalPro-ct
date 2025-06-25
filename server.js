const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const admin = require('firebase-admin');
require('dotenv').config();
const axios = require('axios');
const sdk = require('microsoft-cognitiveservices-speech-sdk');

// Firebase Admin SDK configuration
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

// Initialize Express app
const app = express();
app.use(cors());
app.use(express.json());

// Initialize Firebase Admin SDK
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

// Create HTTP server and Socket.IO
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
});

// Maps for user management
const userSockets = new Map(); // userId -> socketId
const userTokens = new Map();  // userId -> fcmToken

const PORT = process.env.PORT || 3001;
const HOST = process.env.HOST || '192.168.1.9';

const azureKey = process.env.AZURE_SPEECH_KEY;
const azureRegion = process.env.AZURE_SPEECH_REGION;

if (!azureKey || !azureRegion) {
  throw new Error('Azure credentials must be set in .env file');
}

// ===== 2. IN-MEMORY DATA STORAGE =====
let rooms = {};
const activeRecognizers = {};


// ===== UTILITY FUNCTIONS =====

/**
 * Get FCM token from cache or Firestore
 */
async function getFcmToken(userId) {
  if (userTokens.has(userId)) {
    return userTokens.get(userId);
  }
  
  try {
    console.log(`Fetching FCM token for user ${userId} from Firestore...`);
    const userDoc = await db.collection('users').doc(userId).get();
    if (userDoc.exists) {
      const userData = userDoc.data();
      const token = userData?.currentSession?.fcmToken;
      if (token) {
        userTokens.set(userId, token);
        return token;
      }
    }
  } catch (error) {
    console.error(`Error fetching FCM token for user ${userId}:`, error);
  }
  return null;
}

/**
 * Get user info from Firestore
 */
async function getUserInfo(userId) {
  try {
    const userDoc = await db.collection('users').doc(userId).get();
    if (userDoc.exists) {
      return userDoc.data();
    }
  } catch (error) {
    console.error(`Error fetching user info for ${userId}:`, error);
  }
  return null;
}

/**
 * Check if recipient has muted the sender
 */
async function isRecipientMutedSender(recipientId, senderId) {
  try {
    const recipientDoc = await db.collection('users').doc(recipientId).get();
    if (recipientDoc.exists) {
      const userData = recipientDoc.data();
      const blockedUsers = userData?.blockedUsers || [];
      const mutedUsers = userData?.mutedUsers || [];
      return blockedUsers.includes(senderId) || mutedUsers.includes(senderId);
    }
  } catch (error) {
    console.error(`Error checking if recipient ${recipientId} muted sender ${senderId}:`, error);
  }
  return false;
}

// ===== NOTIFICATION HELPER FUNCTIONS =====

function getChannelId(type) {
  const channels = {
    message: 'chat_messages',
    call: 'voice_calls',
    video_call: 'video_calls',
    group_invite: 'group_invites',
    system: 'system_notifications',
    default: 'default_channel'
  };
  return channels[type] || channels.default;
}

function getPriority(type) {
  const highPriorityTypes = ['call', 'video_call', 'emergency'];
  return highPriorityTypes.includes(type) ? 'high' : 'normal';
}

function getVisibility(type) {
  const privateTypes = ['message', 'call'];
  return privateTypes.includes(type) ? 'private' : 'public';
}

function getNotificationTag(type, data) {
  if (type === 'message' && data.chatId) {
    return `chat_${data.chatId}`;
  }
  if (type === 'call' && data.meetingId) {
    return `call_${data.meetingId}`;
  }
  return type;
}

function getCategory(type) {
  const categories = {
    message: 'MESSAGE_CATEGORY',
    call: 'CALL_CATEGORY',
    video_call: 'VIDEO_CALL_CATEGORY',
    default: 'DEFAULT_CATEGORY'
  };
  return categories[type] || categories.default;
}

/**
 * Remove invalid FCM token from cache and database
 */
async function removeInvalidToken(token, error) {
  const isInvalidToken = error?.code === 'messaging/invalid-registration-token' ||
                        error?.code === 'messaging/registration-token-not-registered';
  
  if (isInvalidToken) {
    console.log(`🗑️ Removing invalid FCM token: ${token}`);
    
    for (const [userId, cachedToken] of userTokens.entries()) {
      if (cachedToken === token) {
        userTokens.delete(userId);
        
        try {
          await db.collection('users').doc(userId).update({
            currentSession: admin.firestore.FieldValue.delete(),
            fcmToken: admin.firestore.FieldValue.delete()
          });
          console.log(`🗑️ Removed invalid token from database for user: ${userId}`);
        } catch (dbError) {
          console.error(`Error removing token from database:`, dbError);
        }
        break;
      }
    }
  }
}

// ===== MAIN NOTIFICATION FUNCTIONS =====

/**
 * Send push notification via FCM
 */
async function sendNotificationsBatch(tokens, notificationData, additionalData = {}) {
  const validTokens = Array.isArray(tokens) ? tokens : [tokens];
  const BATCH_SIZE = 500; // FCM limit is 500 tokens per request
  
  // console.log(`📱 Sending notifications to ${validTokens.length} tokens in batches`);
  
  const allResults = [];
  let totalSuccess = 0;
  let totalFailure = 0;

  for (let i = 0; i < validTokens.length; i += BATCH_SIZE) {
    const batch = validTokens.slice(i, i + BATCH_SIZE);
    // console.log(`📦 Processing batch ${Math.floor(i/BATCH_SIZE) + 1}: ${batch.length} tokens`);
    
    try {
      const result = await sendPushNotification(batch, notificationData, additionalData);
      allResults.push(result);
      
      if (result.success) {
        totalSuccess += result.successCount || 0;
        totalFailure += result.failureCount || 0;
      } else {
        totalFailure += batch.length;
      }
    } catch (error) {
      console.error(`❌ Batch ${Math.floor(i/BATCH_SIZE) + 1} failed:`, error);
      totalFailure += batch.length;
    }
  }

  // console.log(`📊 Batch results: ${totalSuccess} success, ${totalFailure} failed`);
  return {
    success: totalSuccess > 0,
    totalSuccess,
    totalFailure,
    batches: allResults.length,
    results: allResults
  };
}
async function sendPushNotification(tokens, notificationData, additionalData = {}) {
  const tokenArray = Array.isArray(tokens) ? tokens : [tokens];
  const validTokens = tokenArray.filter(token => token && typeof token === 'string' && token.trim().length > 0);
  
  if (validTokens.length === 0) {
    console.warn('No valid FCM tokens provided');
    return { success: false, error: 'No valid tokens' };
  }

  const {
    title,
    body,
    icon = 'ic_notification',
    sound = 'default',
    badge,
    imageUrl,
    type = 'default'
  } = notificationData;

  // Prepare data payload
  const dataPayload = {
    type,
    timestamp: Date.now().toString(),
    ...Object.fromEntries(
      Object.entries(additionalData).map(([key, value]) => [
        key,
        typeof value === 'string' ? value : JSON.stringify(value)
      ])
    )
  };

  const notification = {
    title,
    body,
    ...(imageUrl && { imageUrl })
  };

  const android = {
    notification: {
      icon,
      sound,
      channelId: getChannelId(type),
      visibility: getVisibility(type),
      ...(badge && { notificationCount: badge }),
      clickAction: 'FLUTTER_NOTIFICATION_CLICK',
      tag: getNotificationTag(type, additionalData),
    },
    data: dataPayload,
    priority: getPriority(type)
  };

  const apns = {
    payload: {
      aps: {
        alert: { title, body },
        sound,
        ...(badge && { badge }),
        category: getCategory(type),
        'mutable-content': 1,
        'content-available': 1
      }
    },
    fcmOptions: {
      ...(imageUrl && { imageUrl })
    }
  };

  if (validTokens.length > 1) {
    // Send all notifications concurrently instead of multicast
    // console.log(`📱 Sending ${validTokens.length} notifications concurrently...`);
    
    const notificationPromises = validTokens.map(async (token, index) => {
      const message = {
        notification,
        android,
        apns,
        data: dataPayload,
        token: token
      };

      try {
        const response = await admin.messaging().send(message);
        // console.log(`📱 Notification ${index + 1}/${validTokens.length} sent successfully`);
        return { success: true, messageId: response, token };
      } catch (error) {
        console.error(`❌ Failed to send notification ${index + 1}/${validTokens.length}:`, error);
        removeInvalidToken(token, error);
        return { success: false, error: error.message, token };
      }
    });

    try {
      const responses = await Promise.allSettled(notificationPromises);
      
      let successCount = 0;
      let failureCount = 0;
      
      responses.forEach((result, index) => {
        if (result.status === 'fulfilled') {
          if (result.value.success) {
            successCount++;
          } else {
            failureCount++;
          }
        } else {
          failureCount++;
          console.error(`❌ Promise rejected for token ${index}:`, result.reason);
        }
      });
      
      // console.log(`📊 Concurrent notifications: ${successCount} success, ${failureCount} failed`);
      
      return {
        success: successCount > 0,
        successCount,
        failureCount,
        responses: responses.map(r => r.status === 'fulfilled' ? r.value : { success: false, error: r.reason })
      };
    } catch (error) {
      console.error('❌ Error in concurrent notification sending:', error);
      return { success: false, error: error.message };
    }
  } else {
    // Single message
    const message = {
      notification,
      android,
      apns,
      data: dataPayload,
      token: validTokens[0]
    };

    try {
      const response = await admin.messaging().send(message);
      // console.log(`📱 Single notification sent successfully: ${response}`);
      return { success: true, messageId: response };
    } catch (error) {
      console.error('❌ Error sending single notification:', error);
      removeInvalidToken(validTokens[0], error);
      return { success: false, error: error.message };
    }
  }
}

/**
 * Send chat message notification
 */
async function sendChatNotification(chatId, senderId, message, memberIds) {
  try {
    const senderInfo = await getUserInfo(senderId);
    const senderName = senderInfo?.name || senderInfo?.email || 'Someone';
    
    // Get chat info to check muted users
    const chatDoc = await db.collection('chats').doc(chatId).get();
    const chatData = chatDoc.exists ? chatDoc.data() : {};
    const chatName = chatData?.name || 'Chat';
    const isGroup = chatData?.isGroup || false;
    const mutedUsers = chatData?.muted || [];
    
    // console.log(`📨 Sending chat notification for chat ${chatId}`);
    // console.log(`🔇 Muted users in chat: ${mutedUsers.join(', ')}`);
    // console.log(`👤 Sender: ${senderId}`);
    
    const recipientIds = memberIds.filter(id => id !== senderId);
    // console.log(`👥 Recipients: ${recipientIds.join(', ')}`);
    
    for (const recipientId of recipientIds) {
      // Check if SENDER muted this chat (not recipient)
      // Nếu sender đã mute chat, thì sender không nên gửi notification
      if (mutedUsers.includes(recipientId)) {
        // console.log(`🔇 Sender ${senderId} has muted chat ${chatId}, skipping send notifications to  this sender`);
        break; // Skip all recipients since sender muted the chat
      }
      
      // // Check if recipient is online
      // const isOnline = userSockets.has(recipientId);
      // if (isOnline) {
      //   console.log(`🟢 Recipient ${recipientId} is online, skipping push notification`);
      //   continue;
      // }
      
      // Check if recipient muted sender
      const isMutedSender = await isRecipientMutedSender(recipientId, senderId);
      if (isMutedSender) {
        // console.log(`🚫 Recipient ${recipientId} has muted sender ${senderId}, skipping notification`);
        continue;
      }
      
      const token = await getFcmToken(recipientId);
      if (token) {
        const notificationData = {
          title: isGroup ? `${senderName} in ${chatName}` : senderName,
          body: message.length > 100 ? `${message.substring(0, 97)}...` : message,
          type: 'message',
          sound: 'default'
        };
        
        const additionalData = {
          chatId,
          senderId,
          isGroup: isGroup.toString(),
          chatName,
          senderName
        };
        
        // console.log(`📱 Sending message notification to recipient ${recipientId}`);
        await sendPushNotification(token, notificationData, additionalData);
      } else {
        console.log(`⚠️ No FCM token found for recipient ${recipientId}`);
      }
    }
  } catch (error) {
    console.error('Error sending chat notification:', error);
  }
}

/**
 * Send call notification
 */
async function sendCallNotification(meetingId, callerId, memberIds, isVideoCall = false) {
  try {
    const chatDoc = await db.collection('chats').doc(meetingId).get();
    const chatData = chatDoc.exists ? chatDoc.data() : {};
    const callerInfo = await getUserInfo(callerId);
    const callerName = callerInfo?.name || callerInfo?.email || 'Someone';
    
    // console.log(`📞 Sending call notification for meeting ${meetingId}`);
    // console.log(`👤 Caller: ${callerId}`);
    
    const recipientIds = memberIds.filter(id => id !== callerId);
    // console.log(`👥 Recipients: ${recipientIds.join(', ')}`);
    
    const tokens = [];
    const mutedUsers = chatData?.muted || [];
    
    for (const recipientId of recipientIds) {
      if (mutedUsers.includes(recipientId)) {
        // console.log(`🔇 Sender ${senderId} has muted chat ${chatId}, skipping send notifications to  this sender`);
        break; // Skip all recipients since sender muted the chat
      }
      // Check if recipient has muted the caller
      const isMutedCaller = await isRecipientMutedSender(recipientId, callerId);
      if (isMutedCaller) {
        // console.log(`🚫 Recipient ${recipientId} has muted caller ${callerId}, skipping call notification`);
        continue;
      }
      
      const token = await getFcmToken(recipientId);
      if (token) {
        tokens.push(token);
      } else {
        // console.log(`⚠️ No FCM token found for recipient ${recipientId}`);
      }
    }
    
    if (tokens.length > 0) {
      const notificationData = {
        title: `${isVideoCall ? 'Video' : 'Voice'} Call`,
        body: `${callerName} is calling you`,
        type: isVideoCall ? 'video_call' : 'call',
        sound: 'ringtone.wav'
      };
      
      const additionalData = {
        meetingId,
        callerId,
        callerName,
        isVideoCall: isVideoCall.toString(),
        action: 'incoming_call'
      };
      
      // console.log(`📱 Sending call notification to ${tokens.length} users`);
      
      if (tokens.length > 10) {
        // Use batch sending for large groups
        await sendNotificationsBatch(tokens, notificationData, additionalData);
      } else {
        // Use regular sending for small groups
        await sendPushNotification(tokens, notificationData, additionalData);
      }
    } else {
      // console.log('⚠️ No valid tokens found for call notification');
    }
  } catch (error) {
    console.error('Error sending call notification:', error);
  }
}

// ===== SOCKET.IO EVENT HANDLERS =====
/**
 * Lấy tên giọng đọc chuẩn của Azure cho một mã ngôn ngữ.
 * @param {string} langCode - Mã ngôn ngữ (ví dụ: 'en-US', 'ja')
 * @returns {string|null} Tên giọng đọc hoặc null nếu không tìm thấy.
 */
function getVoiceNameForLanguage(langCode) {
    const voiceMap = {
    // Afrikaans
    'af-ZA': 'af-ZA-AdriNeural',

    // Amharic
    'am-ET': 'am-ET-MekdesNeural',

    // Arabic
    'ar-AE': 'ar-AE-FatimaNeural',
    'ar-BH': 'ar-BH-AliNeural',
    'ar-DZ': 'ar-DZ-AminaNeural',
    'ar-EG': 'ar-EG-SalmaNeural',
    'ar-IQ': 'ar-IQ-BasselNeural',
    'ar-JO': 'ar-JO-SanaNeural',
    'ar-KW': 'ar-KW-FahedNeural',
    'ar-LB': 'ar-LB-LaylaNeural',
    'ar-LY': 'ar-LY-ImanNeural',
    'ar-MA': 'ar-MA-JamalNeural',
    'ar-OM': 'ar-OM-AbdullahNeural',
    'ar-QA': 'ar-QA-AmalNeural',
    'ar-SA': 'ar-SA-ZariyahNeural',
    'ar-SY': 'ar-SY-AmanyNeural',
    'ar-TN': 'ar-TN-HediNeural',
    'ar-YE': 'ar-YE-MaryamNeural',

    // Azerbaijani
    'az-AZ': 'az-AZ-BabekNeural',

    // Bulgarian
    'bg-BG': 'bg-BG-KalinaNeural',

    // Bangla
    'bn-BD': 'bn-BD-NabanitaNeural',
    'bn-IN': 'bn-IN-TanishaNeural',

    // Bosnian
    'bs-BA': 'bs-BA-GoranNeural',

    // Catalan
    'ca-ES': 'ca-ES-JoanaNeural',

    // Czech
    'cs-CZ': 'cs-CZ-AntoninNeural',

    // Welsh
    'cy-GB': 'cy-GB-AledNeural',

    // Danish
    'da-DK': 'da-DK-ChristelNeural',

    // German
    'de-AT': 'de-AT-IngridNeural',
    'de-CH': 'de-CH-LeniNeural',
    'de-DE': 'de-DE-KatjaNeural', // Giọng nữ
    // 'de-DE': 'de-DE-ConradNeural', // Giọng nam

    // Greek
    'el-GR': 'el-GR-AthinaNeural',

    // English
    'en-AU': 'en-AU-NatashaNeural',
    'en-CA': 'en-CA-ClaraNeural',
    'en-GB': 'en-GB-LibbyNeural', // Giọng nữ
    // 'en-GB': 'en-GB-RyanNeural', // Giọng nam
    'en-HK': 'en-HK-YanNeural',
    'en-IE': 'en-IE-ConnorNeural',
    'en-IN': 'en-IN-NeerjaNeural',
    'en-KE': 'en-KE-AsiliaNeural',
    'en-NG': 'en-NG-AbeoNeural',
    'en-NZ': 'en-NZ-MitchellNeural',
    'en-PH': 'en-PH-JamesNeural',
    'en-SG': 'en-SG-LunaNeural',
    'en-TZ': 'en-TZ-ElimuNeural',
    'en-US': 'en-US-JennyNeural', // Giọng nữ mặc định
    // 'en-US': 'en-US-GuyNeural', // Giọng nam
    // 'en-US': 'en-US-AriaNeural',
    // 'en-US': 'en-US-DavisNeural',
    'en-ZA': 'en-ZA-LeahNeural',

    // Spanish
    'es-AR': 'es-AR-ElenaNeural',
    'es-BO': 'es-BO-MarceloNeural',
    'es-CL': 'es-CL-LorenzoNeural',
    'es-CO': 'es-CO-SalomeNeural',
    'es-CR': 'es-CR-JuanNeural',
    'es-CU': 'es-CU-ManuelNeural',
    'es-DO': 'es-DO-EmilioNeural',
    'es-EC': 'es-EC-AndreaNeural',
    'es-ES': 'es-ES-ElviraNeural',
    'es-GQ': 'es-GQ-TeresaNeural',
    'es-GT': 'es-GT-AndresNeural',
    'es-HN': 'es-HN-CarlosNeural',
    'es-MX': 'es-MX-DaliaNeural',
    'es-NI': 'es-NI-FedericoNeural',
    'es-PA': 'es-PA-MargaritaNeural',
    'es-PE': 'es-PE-AlexNeural',
    'es-PR': 'es-PR-KarinaNeural',
    'es-PY': 'es-PY-MarioNeural',
    'es-SV': 'es-SV-LorenaNeural',
    'es-US': 'es-US-AlonsoNeural',
    'es-UY': 'es-UY-MateoNeural',
    'es-VE': 'es-VE-PaolaNeural',

    // Estonian
    'et-EE': 'et-EE-AnuNeural',

    // Basque
    'eu-ES': 'eu-ES-AinhoaNeural',

    // Persian (Farsi)
    'fa-IR': 'fa-IR-DilaraNeural',

    // Finnish
    'fi-FI': 'fi-FI-SelmaNeural',

    // Filipino
    'fil-PH': 'fil-PH-BlessicaNeural',

    // French
    'fr-BE': 'fr-BE-CharlineNeural',
    'fr-CA': 'fr-CA-SylvieNeural',
    'fr-CH': 'fr-CH-ArianeNeural',
    'fr-FR': 'fr-FR-DeniseNeural',

    // Irish
    'ga-IE': 'ga-IE-ColmNeural',

    // Galician
    'gl-ES': 'gl-ES-RoiNeural',

    // Gujarati
    'gu-IN': 'gu-IN-DhwaniNeural',

    // Hebrew
    'he-IL': 'he-IL-HilaNeural',

    // Hindi
    'hi-IN': 'hi-IN-SwaraNeural',

    // Croatian
    'hr-HR': 'hr-HR-GabrijelaNeural',

    // Hungarian
    'hu-HU': 'hu-HU-NoemiNeural',

    // Armenian
    'hy-AM': 'hy-AM-AnahitNeural',

    // Indonesian
    'id-ID': 'id-ID-GadisNeural',

    // Icelandic
    'is-IS': 'is-IS-GudrunNeural',

    // Italian
    'it-IT': 'it-IT-ElsaNeural',

    // Japanese
    'ja-JP': 'ja-JP-NanamiNeural',

    // Javanese
    'jv-ID': 'jv-ID-DimasNeural',

    // Georgian
    'ka-GE': 'ka-GE-EkaNeural',

    // Kazakh
    'kk-KZ': 'kk-KZ-AigulNeural',

    // Khmer
    'km-KH': 'km-KH-SreymomNeural',

    // Kannada
    'kn-IN': 'kn-IN-GaganNeural',

    // Korean
    'ko-KR': 'ko-KR-SunHiNeural',

    // Lao
    'lo-LA': 'lo-LA-KeomanyNeural',

    // Lithuanian
    'lt-LT': 'lt-LT-OnaNeural',

    // Latvian
    'lv-LV': 'lv-LV-EveritaNeural',

    // Macedonian
    'mk-MK': 'mk-MK-MarijaNeural',

    // Malayalam
    'ml-IN': 'ml-IN-MidhunNeural',

    // Mongolian
    'mn-MN': 'mn-MN-BataaNeural',

    // Marathi
    'mr-IN': 'mr-IN-AarohiNeural',

    // Malay
    'ms-MY': 'ms-MY-YasminNeural',

    // Maltese
    'mt-MT': 'mt-MT-GraceNeural',

    // Burmese (Myanmar)
    'my-MM': 'my-MM-NilarNeural',

    // Norwegian (Bokmål)
    'nb-NO': 'nb-NO-PernilleNeural',

    // Nepali
    'ne-NP': 'ne-NP-SagarNeural',

    // Dutch
    'nl-BE': 'nl-BE-DenaNeural',
    'nl-NL': 'nl-NL-FennaNeural',

    // Polish
    'pl-PL': 'pl-PL-ZofiaNeural',

    // Pashto
    'ps-AF': 'ps-AF-GulNawazNeural',

    // Portuguese
    'pt-BR': 'pt-BR-FranciscaNeural',
    'pt-PT': 'pt-PT-DuarteNeural',

    // Romanian
    'ro-RO': 'ro-RO-AlinaNeural',

    // Russian
    'ru-RU': 'ru-RU-SvetlanaNeural',

    // Sinhala
    'si-LK': 'si-LK-ThiliniNeural',

    // Slovak
    'sk-SK': 'sk-SK-LukasNeural',

    // Slovenian
    'sl-SI': 'sl-SI-PetraNeural',

    // Somali
    'so-SO': 'so-SO-UbaxNeural',

    // Albanian
    'sq-AL': 'sq-AL-AnilaNeural',

    // Serbian
    'sr-RS': 'sr-RS-SophieNeural',

    // Sundanese
    'su-ID': 'su-ID-JajangNeural',

    // Swedish
    'sv-SE': 'sv-SE-SofieNeural',

    // Swahili
    'sw-KE': 'sw-KE-ZuriNeural',
    'sw-TZ': 'sw-TZ-DaudiNeural',

    // Tamil
    'ta-IN': 'ta-IN-PallaviNeural',
    'ta-LK': 'ta-LK-KumarNeural',
    'ta-MY': 'ta-MY-KaniNeural',
    'ta-SG': 'ta-SG-AnbuNeural',

    // Telugu
    'te-IN': 'te-IN-ShrutiNeural',

    // Thai
    'th-TH': 'th-TH-PremwadeeNeural',

    // Turkish
    'tr-TR': 'tr-TR-EmelNeural',

    // Ukrainian
    'uk-UA': 'uk-UA-PolinaNeural',

    // Urdu
    'ur-IN': 'ur-IN-GulNeural',
    'ur-PK': 'ur-PK-AsadNeural',

    // Uzbek
    'uz-UZ': 'uz-UZ-MadinaNeural',

    // Vietnamese
    'vi-VN': 'vi-VN-HoaiMyNeural', // Giọng nữ
    // 'vi-VN': 'vi-VN-NamMinhNeural', // Giọng nam

    // Chinese
    'zh-CN': 'zh-CN-XiaoxiaoNeural',
    'zh-CN-liaoning': 'zh-CN-liaoning-XiaobeiNeural',
    'zh-CN-shaanxi': 'zh-CN-shaanxi-XiaoniNeural',
    'zh-HK': 'zh-HK-HiuMaanNeural',
    'zh-TW': 'zh-TW-HsiaoChenNeural',

    // Zulu
    'zu-ZA': 'zu-ZA-ThandoNeural',
  };

    // const langPrefix = langCode.split('-')[0].toLowerCase();
    return voiceMap[langCode] || null;
}

/**
 * Gọi API REST của Azure Text-to-Speech để chuyển văn bản thành âm thanh.
 * @param {string} text - Đoạn văn bản cần chuyển đổi.
 * @param {string} language - Mã ngôn ngữ (ví dụ: 'ja-JP').
 * @param {string} voiceName - Tên giọng đọc (ví dụ: 'ja-JP-NanamiNeural').
 * @returns {Promise<Buffer|null>} Buffer chứa dữ liệu audio hoặc null nếu có lỗi.
 */
async function textToSpeech(text, language, voiceName) {
  const ttsUrl = `https://${azureRegion}.tts.speech.microsoft.com/cognitiveservices/v1`;
  const headers = {
    'Ocp-Apim-Subscription-Key': azureKey,
    'Content-Type': 'application/ssml+xml',
    'X-Microsoft-OutputFormat': 'riff-24khz-16bit-mono-pcm',
  };
  const ssml = `
    <speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xml:lang='${language}'>
      <voice name='${voiceName}'>
        ${text}
      </voice>
    </speak>
  `;

  try {
    const response = await axios.post(ttsUrl, ssml, { headers, responseType: 'arraybuffer' });
    return Buffer.from(response.data); // Trả về dạng Buffer
  } catch (error) {
    const errorDetails = error.response?.data?.toString() || error.message;
    console.error(`TTS API Error for lang ${language}: ${errorDetails}`);
    return null;
  }
}

/**
 * Dọn dẹp recognizer cho một phòng.
 * @param {string} meetingId - ID của phòng.
 */
function cleanupRecognizer(meetingId) {
    const recognizerInfo = activeRecognizers[meetingId];
    if (recognizerInfo) {
        // console.log(`[${meetingId}] Cleaning up recognizer.`);
        recognizerInfo.recognizer.stopContinuousRecognitionAsync(
            () => {
                recognizerInfo.recognizer.close();
                recognizerInfo.pushStream.close();
                delete activeRecognizers[meetingId];
            },
            (err) => {
                console.error(`[${meetingId}] Error stopping recognizer:`, err);
                delete activeRecognizers[meetingId];
            }
        );
    }
}

io.on('connection', (socket) => {
  // console.log('🔌 New client connected:', socket.id);

  // Register user with socket and FCM token
  socket.on('register', async ({ userId, fcmToken, from }) => {
    // console.log("from: ", from);
    userSockets.set(userId, socket.id);
    
    if (fcmToken) {
      userTokens.set(userId, fcmToken);
      // console.log(`👤 Registered userId ${userId} with FCM token from client`);
      
      try {
        await db.collection('users').doc(userId).update({ 
          fcmToken,
          lastSeen: admin.firestore.FieldValue.serverTimestamp(),
          isOnline: true
        });
      } catch (error) {
        console.error(`Failed to update FCM token for user ${userId}:`, error);
      }
    } else {
      const token = await getFcmToken(userId);
      if (token) {
        // console.log(`👤 Registered userId ${userId} with FCM token from Firestore`);
      } else {
        console.log(`⚠️ User ${userId} doesn't have FCM token`);
      }
    }
  });

  // Handle message sending
  socket.on('send_message', async ({ chatId, senderId, message, memberIds }) => {
    console.log(`💬 New message in chat ${chatId} from ${senderId}`);
    console.log(`📝 Message: ${message}`);
    console.log(`👥 All members: ${memberIds?.join(', ')}`);
    
    if (memberIds && Array.isArray(memberIds)) {
      const recipientIds = memberIds.filter(id => id !== senderId);
      // console.log(`📤 Will send notifications to: ${recipientIds.join(', ')}`);
      
      if (recipientIds.length > 0) {
        await sendChatNotification(chatId, senderId, message, recipientIds);
      } else {
        console.log(`ℹ️ No recipients to send notifications to`);
      }
    } else {
      console.warn('❌ Invalid memberIds in send_message event');
    }
  });

  // // Handle translation
  // socket.on('send_translation', ({ toUserId, fromUserId, text, lang, isFinal }) => {
  //   const toSocketId = userSockets.get(toUserId);
  //   if (toSocketId) {
  //     io.to(toSocketId).emit('receive_translation', { fromUserId, text, lang, isFinal });
  //     console.log(`➡️ ${fromUserId} → ${toUserId}: ${text} (${lang}) isFinal: ${isFinal}`);
  //   }
  // });

  // Handle call start
  socket.on('start_call', async ({ meetingId, fromUserId, user, memberIds, isVideoCall = false }) => {
    // console.log('📞 start_call', { meetingId, fromUserId, memberIds, isVideoCall });
    
    if (!Array.isArray(memberIds)) {
      console.warn('start_call memberIds is not array:', memberIds);
      return;
    }

    try {
      const meetingRef = db.collection('meetings').doc(meetingId);
      const meetingDoc = await meetingRef.get();

      if (!meetingDoc.exists) {
        console.warn('Meeting not found:', meetingId);
        return;
      }
      if (!rooms[meetingId]) {
        rooms = {};
        rooms[meetingId] = { participants: new Map() };
      }
      rooms[meetingId].participants.set(socket.id, user);

      const currentMembers = meetingDoc.data()?.members || [];
      // console.log('Current members in meeting:', currentMembers);
      // console.log("memberIds:", memberIds);

      const notInCallMembers = memberIds.filter(memberId => {
        if (!memberId || memberId === fromUserId) return false;
        return !currentMembers.some(member => member.uid === memberId);
      });

      if (notInCallMembers.length > 0) {
        // console.log(`📞 Sending call notification to: ${notInCallMembers.join(', ')}`);
        await sendCallNotification(meetingId, fromUserId, notInCallMembers, isVideoCall);
      } else {
        console.log('📞 All members are already in call or no valid recipients');
      }
    } catch (error) {
      console.error('Error handling start_call:', error);
    }
  });
  socket.on('start_speaking', ({ meetingId, user }) => {
    if (!meetingId || !user || activeRecognizers[meetingId]) return;

    console.log(`[${meetingId}] User ${user.uid} starts speaking.`);
    try {
        
        const participants = rooms[meetingId]?.participants;
        if (!participants || participants.size < 2) return;

        const targetLanguages = new Set();
        const voiceMappings = {};

        participants.forEach(p => {
            if (p.uid !== user.uid && p.translateCode) {
                targetLanguages.add(p.translateCode);
                voiceMappings[p.language] = getVoiceNameForLanguage(p.language);
            }
        });
        
        if (targetLanguages.size === 0) return;
        
        // Cấu hình recognizer, lần này không cần cấu hình voice cho TTS
        const speechConfig = sdk.SpeechTranslationConfig.fromSubscription(azureKey, azureRegion);
        speechConfig.speechRecognitionLanguage = user.language;
        targetLanguages.forEach(lang => speechConfig.addTargetLanguage(lang));

        const pushStream = sdk.AudioInputStream.createPushStream();
        const audioConfig = sdk.AudioConfig.fromStreamInput(pushStream);
        const recognizer = new sdk.TranslationRecognizer(speechConfig, audioConfig);

        activeRecognizers[meetingId] = { recognizer, pushStream, speakerId: socket.id };

        const lastSynthesizedText = new Map();
        let fullText = '';
        let lastText = '';
        // console.log("participants:", participants);


        // *** LẮNG NGHE SỰ KIỆN `translating` ĐỂ LẤY TEXT DỊCH ***
        recognizer.recognizing = (s, e) => {
          // console.log(`${meetingId} Recognizing text:`, e.result.text);
          // console.log("user.uid:", user.uid);
          let key;
          for (const [socketId, participant] of participants) {
            if (participant.uid === user.uid) {
              key =  socketId;
              break;
            }
          }
          // console.log("currentUserInParticipants:", participants[key]);
          if (key=== "") return; 
          if (e.result.text) {
            // console.log("sending translating subtitle to current user:", key);

            io.to(key).emit('receive_translating_subtitle', {
              text: e.result.text,
              language: user.translateCode,
            });
          }
          participants.forEach((pUser, pSocketId) => {
            if (e.result.text) {
                const translatedText = e.result.translations.get(pUser.translateCode);
                if (translatedText) {
                  lastSynthesizedText.set(pUser.translateCode, translatedText);
                  io.to(pSocketId).emit('receive_translating_subtitle', {
                    text: translatedText,
                    language: pUser.translateCode,
                  });
                }
            }
          }
          );
            
        };

        recognizer.recognized = (s, e) => {
            // console.log(`[${meetingId}] Final recognized text: "${e.result.text}"`);
            participants.forEach((pUser, pSocketId) => {
                const translatedText = lastSynthesizedText.get(pUser.translateCode);
                if (!translatedText) return;
                io.to(pSocketId).emit('receive_final_subtitle', {
                    text: translatedText,
                    language: pUser.translateCode,
                });

                // console.log(`[${meetingId}] Sending translation to ${pUser.translateCode}: "${translatedText}"`);
                console.log( "voiceName",voiceMappings[pUser.language]);
                
                textToSpeech(translatedText, pUser.translateCode, voiceMappings[pUser.language])
                    .then(audioBuffer => {
                        if (audioBuffer) {
                          // console.log("audioBuffer:", audioBuffer);
                          // console.log("audioBuffer length:", audioBuffer.length);
                            io.to(pSocketId).emit('receive_translated_audio', {
                                audio: audioBuffer.toString('base64'),
                                language: pUser.translateCode,
                            });
                            // console.log(`[${meetingId}] Sent audio for ${pSocketId}: "${translatedText}"`);
                        }
                    }
                    )
                    .catch(err => {
                        console.error(`[${meetingId}] Error in TTS for ${pUser.translateCode}:`, err);
                    } 
                );
            });
            // Xóa text đã lưu để bắt đầu câu mới
            lastSynthesizedText.clear();
        };

        recognizer.canceled = (s, e) => {
            console.error(`[${meetingId}] RECOGNIZER CANCELED: ${e.reason} - ${e.errorDetails}`);
            cleanupRecognizer(meetingId);
        };
        
        recognizer.startContinuousRecognitionAsync();
        console.log(`[${meetingId}] Recognizer started with HYBRID architecture.`);

    } catch (error) {
        console.error(`[${meetingId}] FATAL ERROR in start_speaking:`, error);
        delete activeRecognizers[meetingId];
    }
  });
  socket.on('send_audio_chunk', ({meetingId, audioChunk }) => {
    const recognizerInfo = activeRecognizers[meetingId];
    if (recognizerInfo && recognizerInfo.speakerId === socket.id) {
      recognizerInfo.pushStream.write(audioChunk);
    }
  });
  socket.on('stop_speaking', ({meetingId}) => {
    cleanupRecognizer(meetingId);
  });
  socket.on('leave_call', ({ meetingId, user }) => {
    if (rooms[meetingId]) {
      rooms[meetingId].participants.delete(socket.id);
      
      if (activeRecognizers[meetingId]?.speakerId === socket.id) {
        cleanupRecognizer(meetingId);
      }
      
      if (rooms[meetingId].participants.size === 0) {
        delete rooms[meetingId];
      } else {
        io.to(meetingId).emit('user_left', user);
        // what is user_left 

      }
    } else {
      console.warn(`Meeting ${meetingId} not found for user`);
    }
  })
  // Handle disconnect
  socket.on('disconnect', async () => {
    let disconnectedUserId = null;
    
    for (const [userId, sId] of userSockets.entries()) {
      if (sId === socket.id) {
        userSockets.delete(userId);
        disconnectedUserId = userId;
        console.log(`❌ Disconnected ${userId} (${socket.id})`);
        // remove participant has disconnected from rooms
        // break;
        rooms = Object.fromEntries(
          Object.entries(rooms).map(([roomId, room]) => {
            room.participants.delete(socket.id);
            if (room.participants.size === 0) {
              return [roomId, null]; // Remove empty rooms
            }
            return [roomId, room];
          }
        ));

        
        try {
          await db.collection('users').doc(userId).update({
            isOnline: false,
            lastSeen: admin.firestore.FieldValue.serverTimestamp()
          });
          const { meetingId, user } = socket.data;
          if (meetingId && user && rooms[meetingId]) {
              console.log(`🔌 User ${user.uid} disconnected: ${socket.id}`);
              rooms[meetingId].participants.delete(socket.id);
              
              if (activeRecognizers[meetingId]?.speakerId === socket.id) {
                  cleanupRecognizer(meetingId);
              }

              if (rooms[meetingId].participants.size === 0) {
                  delete rooms[meetingId];
              } else {
                  io.to(meetingId).emit('user_left', user);
              }
          }

        } catch (error) {
          console.error(`Error updating offline status for ${userId}:`, error);
        }
        break;
      }
    }
  });
});

// ===== REST API ENDPOINTS =====

// Health check
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    connectedUsers: userSockets.size 
  });
});

// Test notification
app.post('/api/test-notification', async (req, res) => {
  const { userId, title, body, type = 'test' } = req.body;
  
  try {
    const token = await getFcmToken(userId);
    if (!token) {
      return res.status(404).json({ error: 'User token not found' });
    }
    
    const result = await sendPushNotification(
      token,
      { title, body, type },
      { test: 'true' }
    );
    
    res.json({ success: true, result });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Test message notification
app.post('/api/test-message-notification', async (req, res) => {
  const { chatId, senderId, message, memberIds } = req.body;
  
  try {
    await sendChatNotification(chatId, senderId, message, memberIds);
    res.json({ success: true, message: 'Message notification sent' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug muted users
app.post('/api/debug-muted-users', async (req, res) => {
  const { chatId } = req.body;
  
  try {
    const chatDoc = await db.collection('chats').doc(chatId).get();
    if (chatDoc.exists) {
      const chatData = chatDoc.data();
      res.json({
        chatId,
        members: chatData?.members || [],
        mutedUsers: chatData?.muted || [],
        chatName: chatData?.name || 'Unknown',
        isGroup: chatData?.isGroup || false
      });
    } else {
      res.status(404).json({ error: 'Chat not found' });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});


// // ===== 4. SOCKET.IO LOGIC =====
// io.on('connection', (socket) => {
//   console.log(`🔌 User connected: ${socket.id}`);

//   // --- Event: Client tham gia phòng họp ---
//   socket.on('join_meeting', ({ meetingId, user }) => {

//     if (!rooms[meetingId]) {
//       rooms[meetingId] = { participants: new Map() };
//     }
//     rooms[meetingId].participants.set(socket.id, user);

//     console.log(`[${meetingId}] User ${user.uid} socket_id ${socket.id} joined. Total: ${rooms[meetingId].participants.size}`);
//     socket.to(meetingId).emit('user_joined', user);
//   });
 

//   // --- Event: Client bắt đầu nói ---
//   socket.on('start_speaking', ({ meetingId, user }) => {
//     if (!meetingId || !user || activeRecognizers[meetingId]) return;

//     console.log(`[${meetingId}] User ${user.uid} starts speaking.`);
    
//     try {
//         const participants = rooms[meetingId]?.participants;
//         if (!participants || participants.size < 2) return;

//         const targetLanguages = new Set();
//         const voiceMappings = {};

//         participants.forEach(p => {
//             if (p.uid !== user.uid && p.translateCode) {
//                 targetLanguages.add(p.translateCode);
//                 voiceMappings[p.translateCode] = getVoiceNameForLanguage(p.translateCode);
//             }
//         });
        
//         if (targetLanguages.size === 0) return;
        
//         // Cấu hình recognizer, lần này không cần cấu hình voice cho TTS
//         const speechConfig = sdk.SpeechTranslationConfig.fromSubscription(azureKey, azureRegion);
//         speechConfig.speechRecognitionLanguage = user.language;
//         targetLanguages.forEach(lang => speechConfig.addTargetLanguage(lang));

//         const pushStream = sdk.AudioInputStream.createPushStream();
//         const audioConfig = sdk.AudioConfig.fromStreamInput(pushStream);
//         const recognizer = new sdk.TranslationRecognizer(speechConfig, audioConfig);

//         activeRecognizers[meetingId] = { recognizer, pushStream, speakerId: socket.id };

//         const lastSynthesizedText = new Map();
//         let fullText = '';
//         let lastText = '';
//         console.log("participants:", participants);


//         // *** LẮNG NGHE SỰ KIỆN `translating` ĐỂ LẤY TEXT DỊCH ***
//         recognizer.recognizing = (s, e) => {
//           console.log(`${meetingId} Recognizing text:`, e.result.text);
//           participants.forEach((pUser, pSocketId) => {
//             if (e.result.text) {
//                 const translatedText = e.result.translations.get(pUser.translateCode);
//                 if (translatedText) {
//                   lastSynthesizedText.set(pUser.translateCode, translatedText);
//                   io.to(pSocketId).emit('receive_translating_subtitle', {
//                     text: translatedText,
//                     language: pUser.translateCode,
//                   });
//                 }
//             }
//           }
//           );
            
//         };

//         recognizer.recognized = (s, e) => {
//             console.log(`[${meetingId}] Final recognized text: "${e.result.text}"`);
//             participants.forEach((pUser, pSocketId) => {
//                 const translatedText = lastSynthesizedText.get(pUser.translateCode);
//                 if (!translatedText) return;
//                 io.to(pSocketId).emit('receive_final_subtitle', {
//                     text: translatedText,
//                     language: pUser.translateCode,
//                 });

//                 console.log(`[${meetingId}] Sending translation to ${pUser.translateCode}: "${translatedText}"`);
                
//                 textToSpeech(translatedText, pUser.translateCode, voiceMappings[pUser.translateCode])
//                     .then(audioBuffer => {
//                         if (audioBuffer) {
//                           console.log("audioBuffer:", audioBuffer);
//                           console.log("audioBuffer length:", audioBuffer.length);
//                             io.to(pSocketId).emit('receive_translated_audio', {
//                                 audio: audioBuffer.toString('base64'),
//                                 language: pUser.translateCode,
//                             });
//                             console.log(`[${meetingId}] Sent audio for ${pSocketId}: "${translatedText}"`);
//                         }
//                     }
//                     )
//                     .catch(err => {
//                         console.error(`[${meetingId}] Error in TTS for ${pUser.translateCode}:`, err);
//                     } 
//                 );
//             });
//             // Xóa text đã lưu để bắt đầu câu mới
//             lastSynthesizedText.clear();
//         };

//         recognizer.canceled = (s, e) => {
//             console.error(`[${meetingId}] RECOGNIZER CANCELED: ${e.reason} - ${e.errorDetails}`);
//             cleanupRecognizer(meetingId);
//         };
        
//         recognizer.startContinuousRecognitionAsync();
//         console.log(`[${meetingId}] Recognizer started with HYBRID architecture.`);

//     } catch (error) {
//         console.error(`[${meetingId}] FATAL ERROR in start_speaking:`, error);
//         delete activeRecognizers[meetingId];
//     }
//   });


//   // // --- Event: Client gửi mẩu audio ---
//   // socket.on('send_audio_chunk', ({meetingId, audioChunk }) => {
//   //   const recognizerInfo = activeRecognizers[meetingId];
//   //   if (recognizerInfo && recognizerInfo.speakerId === socket.id) {
//   //     recognizerInfo.pushStream.write(audioChunk);
//   //   }
//   // });


//   // // --- Event: Client ngừng nói ---
//   // socket.on('stop_speaking', ({meetingId}) => {
//   //   cleanupRecognizer(meetingId);
//   // });


//   // socket.on('disconnect', () => {
//   //   const { meetingId, user } = socket.data;
//   //   if (meetingId && user && rooms[meetingId]) {
//   //       console.log(`🔌 User ${user.uid} disconnected: ${socket.id}`);
//   //       rooms[meetingId].participants.delete(socket.id);
        
//   //       if (activeRecognizers[meetingId]?.speakerId === socket.id) {
//   //           cleanupRecognizer(meetingId);
//   //       }

//   //       if (rooms[meetingId].participants.size === 0) {
//   //           delete rooms[meetingId];
//   //       } else {
//   //           io.to(meetingId).emit('user_left', user);
//   //       }
//   //   }
//   // });
// });


// ===== 5. START SERVER =====
app.get('/', (req, res) => {
  res.send('Hybrid Voice Translator Server is running!');
});

server.listen(PORT, () => {
  console.log(`🚀 Socket server running on ${HOST}:${PORT}`);
});