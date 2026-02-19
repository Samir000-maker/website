// PM2 Ecosystem Configuration for Clustered WebRTC Server
// Production-ready clustering with automatic resource management

module.exports = {
    apps: [{
        name: 'webrtc-server',
        script: './server.js',

        // CLUSTERING CONFIGURATION
        instances: 'max',  // Use all CPU cores (or set specific number like 4)
        exec_mode: 'cluster',  // Enable cluster mode

        // ENVIRONMENT
        env: {
            NODE_ENV: 'development',
            PORT: 3000
        },
        env_production: {
            NODE_ENV: 'production',
            PORT: 3000
        },

        // RESOURCE LIMITS
        max_memory_restart: '1G',  // Restart if memory exceeds 1GB

        // LOGGING
        error_file: './logs/pm2-error.log',
        out_file: './logs/pm2-out.log',
        log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
        merge_logs: true,  // Merge logs from all instances

        // RESTART BEHAVIOR
        autorestart: true,  // Auto-restart on crash
        watch: false,  // Don't watch file changes in production
        max_restarts: 10,  // Max restarts within min_uptime
        min_uptime: '10s',  // Min uptime before considering it stable

        // GRACEFUL SHUTDOWN
        kill_timeout: 5000,  // Wait 5s for graceful shutdown before force kill
        wait_ready: true,  // Wait for process to signal it's ready
        listen_timeout: 10000,  // Timeout for ready signal

        // INSTANCE SETTINGS
        instance_var: 'INSTANCE_ID',  // Expose instance ID as env var

        // MONITORING
        pmx: true,  // Enable PM2 monitoring

        // ADVANCED OPTIONS
        node_args: '--max-old-space-size=1024',  // Node.js memory limit (1GB)
    }]
}
