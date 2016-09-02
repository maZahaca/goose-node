const kue = require('kue');
const debug = require('debug')('kueHelper');

module.exports = {
    init: function () {
        const redisHost = process.env.REDIS_ENV_REDIS_HOST || 'redis';
        const redisPort = process.env.REDIS_ENV_REDIS_PORT || '6379';
        const redisPass = process.env.REDIS_ENV_REDIS_PASS;

        const queue = kue.createQueue({
            redis: {
                host: redisHost,
                port: redisPort,
                auth: redisPass
            }
        });
        queue.on('error', function (err) {
            debug('Queue error', err);
        });
        debug('Kue initialized %s:%s', redisHost, redisPort);
        return queue;
    }
};
