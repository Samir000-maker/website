// cloudflare-storage.js
import AWS from 'aws-sdk';
import { v4 as uuidv4 } from 'uuid';
import config from './config.js';

// R2 S3-compatible client (PRIVATE endpoint)
const s3 = new AWS.S3({
  endpoint: config.CLOUDFLARE_ENDPOINT,
  accessKeyId: config.ACCESS_KEY,
  secretAccessKey: config.SECRET_KEY,
  signatureVersion: 'v4',
  s3ForcePathStyle: true,
});

const ATTACHMENT_EXTENSION_BY_MIME = {
  'image/jpeg': 'jpg',
  'image/png': 'png',
  'image/gif': 'gif',
  'image/webp': 'webp',
  'image/bmp': 'bmp',
  'image/tiff': 'tiff',
  'image/svg+xml': 'svg',
  'video/mp4': 'mp4',
  'video/webm': 'webm',
  'video/quicktime': 'mov',
  'video/ogg': 'ogv',
  'video/x-msvideo': 'avi',
  'video/x-matroska': 'mkv',
  'video/mpeg': 'mpeg',
  'application/pdf': 'pdf',
  'application/msword': 'doc',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
  'application/vnd.ms-excel': 'xls',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
  'application/vnd.ms-powerpoint': 'ppt',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx',
  'text/plain': 'txt',
  'application/rtf': 'rtf',
  'application/zip': 'zip',
  'application/x-zip-compressed': 'zip'
};

function sanitizeFileName(input = 'file') {
  const sanitized = String(input)
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80);

  return sanitized || 'file';
}

function extractExtension(originalName = '') {
  const parts = String(originalName).split('.');
  if (parts.length < 2) return null;
  const ext = parts.pop().toLowerCase().replace(/[^a-z0-9]/g, '');
  return ext || null;
}

function resolveAttachmentExtension(mimeType = '', originalName = '') {
  const normalizedMime = String(mimeType || '').toLowerCase().trim();
  if (ATTACHMENT_EXTENSION_BY_MIME[normalizedMime]) {
    return ATTACHMENT_EXTENSION_BY_MIME[normalizedMime];
  }

  const fromName = extractExtension(originalName);
  if (fromName) return fromName;

  return 'bin';
}

function parseRange(rangeHeader, totalLength) {
  if (!rangeHeader || typeof rangeHeader !== 'string' || !rangeHeader.startsWith('bytes=')) {
    return null;
  }

  const rawRange = rangeHeader.replace('bytes=', '').split(',')[0].trim();
  const [startStr, endStr] = rawRange.split('-');

  let start = Number.parseInt(startStr, 10);
  let end = endStr ? Number.parseInt(endStr, 10) : totalLength - 1;

  if (Number.isNaN(start)) {
    // Suffix range: "bytes=-500"
    const suffixLength = Number.parseInt(endStr, 10);
    if (Number.isNaN(suffixLength) || suffixLength <= 0) {
      const err = new Error('Invalid range');
      err.code = 'INVALID_RANGE';
      throw err;
    }
    start = Math.max(totalLength - suffixLength, 0);
    end = totalLength - 1;
  }

  if (Number.isNaN(end) || end < start || start < 0 || end >= totalLength) {
    const err = new Error('Invalid range');
    err.code = 'INVALID_RANGE';
    throw err;
  }

  return { start, end };
}

/**
 * Upload profile picture to Cloudflare R2
 * RETURNS: string (public URL)
 */
export async function uploadProfilePicture(fileBuffer, mimeType, userId) {
  try {
    if (!config.CLOUDFLARE_ENDPOINT || !config.BUCKET_NAME || !config.ACCESS_KEY || !config.SECRET_KEY || !config.R2_PUBLIC_URL) {
      const err = new Error('Storage not configured');
      err.code = 'STORAGE_NOT_CONFIGURED';
      throw err;
    }

    const extension = mimeType?.split('/')[1] || 'png';
    const fileName = `profiles/${userId}-${uuidv4()}.${extension}`;

    // Upload to R2 (NO ACL)
    await s3.upload({
      Bucket: config.BUCKET_NAME,
      Key: fileName,
      Body: fileBuffer,
      ContentType: mimeType,
    }).promise();

    // Build PUBLIC r2.dev URL (NO encoding, NO hardcoding filename)
    const publicBase = config.R2_PUBLIC_URL.replace(/\/+$/, '');
    const publicUrl = `${publicBase}/${fileName}`;

    return publicUrl;
  } catch (err) {
    console.error('❌ R2 upload failed:', err);
    if (err && err.code === 'STORAGE_NOT_CONFIGURED') throw err;
    const e = new Error('Profile picture upload failed');
    e.code = err && err.code ? err.code : 'R2_UPLOAD_FAILED';
    throw e;
  }
}

/**
 * Upload chat attachment to Cloudflare R2
 * RETURNS: { fileId, key, publicUrl, mimeType, size }
 */
export async function uploadChatAttachment(fileBuffer, mimeType, originalName, roomId, userId, preferredFileId = null) {
  try {
    if (!fileBuffer || !Buffer.isBuffer(fileBuffer) || fileBuffer.byteLength === 0) {
      throw new Error('Attachment buffer is empty');
    }
    if (!roomId || !userId) {
      throw new Error('roomId and userId are required');
    }

    const fileId = preferredFileId || `file_${roomId}_${Date.now()}_${uuidv4().replace(/-/g, '').slice(0, 8)}`;
    const extension = resolveAttachmentExtension(mimeType, originalName);
    const safeBaseName = sanitizeFileName(originalName || `attachment.${extension}`);
    const safeFileName = safeBaseName.includes('.')
      ? safeBaseName
      : `${safeBaseName}.${extension}`;

    const key = `attachments/${roomId}/${userId}/${fileId}_${safeFileName}`;
    const normalizedMime = (mimeType && String(mimeType).trim()) || 'application/octet-stream';

    await s3.upload({
      Bucket: config.BUCKET_NAME,
      Key: key,
      Body: fileBuffer,
      ContentType: normalizedMime,
      CacheControl: 'public, max-age=31536000, immutable'
    }).promise();

    const publicBase = config.R2_PUBLIC_URL.replace(/\/+$/, '');
    const publicUrl = `${publicBase}/${key}`;

    return {
      fileId,
      key,
      publicUrl,
      mimeType: normalizedMime,
      size: fileBuffer.byteLength
    };
  } catch (err) {
    console.error('❌ Chat attachment upload failed:', err);
    throw new Error('Chat attachment upload failed');
  }
}

/**
 * Stream attachment from R2 with optional byte-range support.
 */
export async function getChatAttachmentStream(storageKey, rangeHeader = null) {
  if (!storageKey) {
    const err = new Error('storageKey is required');
    err.code = 'MISSING_KEY';
    throw err;
  }

  const head = await s3.headObject({
    Bucket: config.BUCKET_NAME,
    Key: storageKey
  }).promise();

  const totalLength = Number(head.ContentLength || 0);
  const contentType = head.ContentType || 'application/octet-stream';
  const parsedRange = parseRange(rangeHeader, totalLength);

  const getObjectParams = {
    Bucket: config.BUCKET_NAME,
    Key: storageKey
  };

  let statusCode = 200;
  let contentLength = totalLength;
  let contentRange = null;

  if (parsedRange) {
    const { start, end } = parsedRange;
    getObjectParams.Range = `bytes=${start}-${end}`;
    statusCode = 206;
    contentLength = (end - start) + 1;
    contentRange = `bytes ${start}-${end}/${totalLength}`;
  }

  const stream = s3.getObject(getObjectParams).createReadStream();

  return {
    stream,
    statusCode,
    contentType,
    contentLength,
    totalLength,
    contentRange,
    etag: head.ETag || null,
    lastModified: head.LastModified || null
  };
}

/**
 * Delete chat attachment from R2 by storage key
 */
export async function deleteChatAttachmentByKey(storageKey) {
  if (!storageKey || typeof storageKey !== 'string') return;
  try {
    await s3.deleteObject({
      Bucket: config.BUCKET_NAME,
      Key: storageKey,
    }).promise();
  } catch (err) {
    console.error('❌ R2 delete attachment failed:', storageKey, err);
  }
}

/**
 * Delete profile picture using stored URL
 */
export async function deleteProfilePicture(pfpUrl) {
  try {
    const url = new URL(pfpUrl);
    let key = url.pathname.replace(/^\/+/, '');

    // Strip bucket name if ever present
    if (key.startsWith(`${config.BUCKET_NAME}/`)) {
      key = key.slice(config.BUCKET_NAME.length + 1);
    }

    await s3.deleteObject({
      Bucket: config.BUCKET_NAME,
      Key: key,
    }).promise();

    console.log('✅ Profile picture deleted:', key);
  } catch (err) {
    console.error('❌ R2 delete failed:', err);
  }
}

/**
 * Default profile picture
 */
export function getDefaultProfilePicture() {
  return 'https://ui-avatars.com/api/?name=User&background=367d7d&color=ffffff&size=200';
}
