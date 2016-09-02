'use strict';

/**
 * @typedef {object} JobData
 * @property {object} request Parsing request
 * @property {string} request.url URL for parsing.
 * @property {object} request.rules Parsing rules. See more https://github.com/redco/goose-parser#parse-rules
 * @property {object} request.options Environment options. See more https://github.com/redco/goose-parser#environments
 * @property {object} request.actions Actions to execute before parsing. See more https://github.com/redco/goose-parser#actions
 * @property {object} request.pagination Pagination rules. See more https://github.com/redco/goose-parser#pagination
 */

/**
 * @typedef {object} Job
 * @property {JobData} data job data
 */

const debug = require('debug')('parser-node');
const GooseParser = require('goose-parser');
const kue = require('kue');
const _ = require('lodash');
const kueHelper = require('./tools/kueHelper');
const Parser = GooseParser.Parser;
const PhantomEnvironment = GooseParser.PhantomEnvironment;
const domain = require('domain');
const path = require('path');
const url = require('url');

const queue = kueHelper.init();
const QUEUE_NAME = process.env.QUEUE_NAME || 'parser-default';
const MAX_MEMORY_LIMIT = (process.env.GOOSE_MEMORY_LIMIT || 256) * 1024 * 1024;
const TIME_LIMIT_FOR_JOB = process.env.TIME_LIMIT_FOR_JOB || 2 * 60 * 1000; // 2 min by default

const defaultOptions = {
    screen: require('./options/viewports'),
    userAgent: require('./options/userAgents'),
    snapshot: false,
    loadImages: true,
    webSecurity: false
};

debug('Connecting to %s queue', QUEUE_NAME);
queue.process(
    QUEUE_NAME,

    /**
     @param {Job} job
     @param {function} done
     */
    (job, done) => {
        debug('New task on queue %s with data %o', QUEUE_NAME, job.data);

        const envOptions = _.defaults(_.clone(job.data.request.options) || {}, defaultOptions);
        envOptions.url = encodeURI(job.data.request.url);

        const env = new PhantomEnvironment(envOptions);
        const finishJob = wrapJobWithTimeLimit(env, job, done);
        checkMemoryUsage();

        const domainInstance = domain.create();
        domainInstance.on('error', function(e) {
            debug('Error had happened: %s %s', e.message, e.stack);
            finishJob(e);
        });
        domainInstance.run(function() {
            parse(env, job.data.request)
                .then(
                    result => {
                        debug('Work is done!');
                        finishJob(null, {result});
                    },
                    e => {
                        debug('Parsing error: %s %s', e.message, e.stack);
                        finishJob(e);
                    });
        });
    });

function parse(env, jobRequest) {
    const parserOptions = {
        environment: env
    };
    if (jobRequest.pagination) {
        parserOptions.pagination = jobRequest.pagination;
    }
    const parser = new Parser(parserOptions);

    return parser.parse({
        actions: jobRequest.actions,
        rules: jobRequest.rules,
        transform: jobRequest.transform,
        rulesParams: jobRequest.rulesParams
    });
}

function wrapJobWithTimeLimit(env, job, done) {
    let jobIsDone = false;
    const jobTimeout = setTimeout(() => finishJob(new Error(`Time limit ${TIME_LIMIT_FOR_JOB} exceeded, killing job`)), TIME_LIMIT_FOR_JOB);

    const finishJob = function doneJob(e, results) {
        clearTimeout(jobTimeout);

        if (jobIsDone) {
            return;
        }
        jobIsDone = true;

        cleanEnv(env, e)
            .then(
                () => done(e, results),
                () => done(e, results)
            );
    };

    return finishJob;
}

function cleanEnv(env, e) {
    if (e) {
        return env.tearDown();
    }

    return Promise.resolve
}

function checkMemoryUsage() {
    const memoryUsage = process.memoryUsage().rss;
    debug('Memory used: %o', memoryUsage);

    if (memoryUsage > MAX_MEMORY_LIMIT) {
        debug('Memory limit exceeded');
        queue.shutdown(TIME_LIMIT_FOR_JOB, function(err) {
            console.log('Kue shutdown: ', err || '');
            process.exit(0);
        });
    }
}
