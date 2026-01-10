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

/**
 * Upload profile picture to Cloudflare R2
 * RETURNS: string (public URL)
 */
export async function uploadProfilePicture(fileBuffer, mimeType, userId) {
  try {
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
    throw new Error('Profile picture upload failed');
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
