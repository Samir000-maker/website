
import Redis from 'ioredis';

// Self-contained recovery script
// const redisHost = '205.198.72.90';
// const redisPort = 6379;
const redisHost = 'redis-16396.c80.us-east-1-2.ec2.cloud.redislabs.com';
const redisPort = 16396;
const redisPassword = 'samir16121?';
const redisUrl = `redis://:${encodeURIComponent(redisPassword)}@${redisHost}:${redisPort}`;

const redis = new Redis(redisUrl);

async function fixRedis() {
    console.log('🔗 Connecting to Redis host:', redisHost);
    try {
        const result = await redis.config('SET', 'stop-writes-on-bgsave-error', 'no');
        console.log('✅ Result of CONFIG SET stop-writes-on-bgsave-error no:', result);

        // Perform a test write to verify
        const testKey = 'recovery_test_' + Date.now();
        await redis.set(testKey, 'SUCCESS');
        const testVal = await redis.get(testKey);
        console.log('📝 Test write/read successful. Val:', testVal);
        await redis.del(testKey);

        console.log('🚀 Redis recovery complete. Writes should now be enabled.');
        process.exit(0);
    } catch (error) {
        console.error('❌ Failed to fix Redis:', error);
        process.exit(1);
    }
}

fixRedis();
