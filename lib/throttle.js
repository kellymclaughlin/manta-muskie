/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var vasync = require('vasync');
var uuid = require('node-uuid');
var util = require('util');
var mod_url = require('url');
var fs = require('fs');
var dtrace = require('dtrace-provider');
var restify = require('restify');
var jsprim = require('jsprim');

require('./errors');

/*
 * High Level Operation
 *
 * This module exports a 'wait' method, which serves as the entry point
 * to all throttling operations the module provides. Users of the module
 * simply call the 'wait' function in a request-processing code path,
 * passing in a callback representing the work required to handle the
 * request. Currently, 'req' and 'res' arguments are also supplied
 * because the throttle is plugged in as restify middleware in muskie,
 * future iterations of this throttle will aim to be generic across all
 * communication protocols, requiring only an argumentless work function
 * as input.
 *
 * The operation of wait can be summarized as follows:
 * - Note the time at which a request arrives.
 * - Figure out how long it's been since the request rate was
 *   last checked.
 * - If it's been long enough (rateCheckInterval), compute the
 *   request rate over this period and cache it as the most
 *   recent request rate.
 * - If at any point the most recent request rate is higher
 *   than the request rate capacity, and we've queued up more
 *   than 'queueTolerance' requests, respond to the client with
 *   an indication that the request will not be processed.
 * - If the most recent request rate is not higher than the
 *   configurable threshold, queue the current reuqest.
 *
 * The module also computes request latencies for all processed requests.
 * These latencies are defined to be the time interval between the point
 * at which the request is queued, to the point at which it's worker
 * function returns, indicating it has been handled by muskie.
 *
 * The fact that the throttle uses both a concurrency value for the request
 * queue as well as a request rate capacity implies that the throttle can
 * control two dimensions of how muskie operates:
 * - The volume of requests muskie will attempt to handle concurrently.
 * - The rate at which muskie can accept new requests.
 *
 * Overview of Tunables and Tradeoffs
 *
 * requestRateCapacity - the soft capacity on requests-per-second. If
 * the request rate observed by the throttle surpasses this value, the
 * throttle will queue up to 'queueTolerance' requests before returning
 * 429s to the client.
 *
 * The higher the request rate capacity, the less likely we are to throttle
 * requests under load. However, we also incur the risk of overloading
 * manta with a capacity that is too high.
 *
 * rateCheckInterval - the amount of time, in seconds, that the throttle
 * should wait before checking the request rate again. The request rate is
 * measured only for a given check interval to avoid past measurements from
 * skewing the throttle's notion of what load manta is currently under.
 *
 * The lower the rate check interval, the more accurate a figure we get for
 * the request rate at any given instant. The higher the rate check interval,
 * the more likely it is that the throttle will not respond quickly enough
 * to increased load.
 *
 * queueTolerance - after the requestRateCapacity is reached, the throttle
 * will allow up to 'queueTolerance' requests to be put in the pending state
 * before returning 429s.
 *
 * The higher the queue tolernace, the less likely it is that we drop
 * requests in the event of a particular bursty time interval. Having an
 * exceptionally high queue tolerance implies increased request latency
 * in a situation where manta is already probably under duress. The lower
 * the queue tolernace, the more likely it is that we throttle requests.
 * This tunable can be thought of as a 'last line of defense' against
 * throttling requests.
 *
 * concurrency - the number of slots the request queue has for scheduling
 * request-handling worker callbacks concurrently. When all the slots are
 * filled, the request queue will starting putting callbacks in the 'pending'
 * state.
 *
 * The higher the concurrency value, the more requests we can process at
 * once. Currently - muskie operates with concurrency == infinity because it
 * immediately schedules the worker function upon request receipt. Having
 * a lower concurrency value makes it more likely that requests will spend
 * time in the queue, leading to increased latency and a higher memory
 * foortprint.
 *
 * enabled - if true, the throttle will queue and throttle requests as needed.
 * This can be thought of as an "active" throttle mode. If false, the throttle
 * will do everything BUT queue and throttle requests. This can be thought of
 * as a "passive" throttle mode. A passive throttle will track the request rate
 * and fire the dtrace probes described below. This means that with a passive
 * throttle we can determine when a request _would_ have been throttled,
 * without actually throttling it.
 */

///--- Exports

module.exports = {

    createThrottle: function createThrottle(options) {
        return (new Throttle(options));
    },

    throttleHandler: function (throttle) {
        function doThrottle(req, res, next) {
            throttle.wait(req, res, next);
        }
        return (doThrottle);
    }

};

/*
 * The throttle object maintains all the state used by the throttle. This state
 * consists of the tunables described above in addition to dtrace probes that
 * help to describe the runtime operation of the throttle. Structuring the
 * throttle as an object allows us to potentially instantiate multiple
 * throttles for different communication abstractions in the same service.
 */
function Throttle(options) {
    assert.bool(options.enabled, 'options.enabled');
    assert.number(options.concurrency, 'options.concurrency');
    assert.ok(options.concurrency > 0, 'concurrency must positive');
    assert.number(options.requestRateCapacity,
            'options.requestRateCapacity');
    assert.ok(options.requestRateCapacity > 0, 'requestRateCapacity must ' +
            'be positive');
    assert.number(options.rateCheckInterval,
            'options.rateCheckInterval');
    assert.ok(options.rateCheckInterval > 0, 'rateCheckInterval must be ' +
            'positive');
    assert.number(options.queueTolerance, 'options.queueTolerance');
    assert.ok(options.queueTolerance > 0, 'queueTolerance must be positive');
    assert.optionalObject(options.log, 'options.log');

    if (options.log) {
        this.log = options.log;
    } else {
        this.log = bunyan.createLogger({ name: 'throttle' });
    }

    this.dtp = dtrace.createDTraceProvider('muskie-throttle');

    this.throttle_probes = {
        // number of 'pending' requests in the request queue
        request_received: this.dtp.addProbe('request_received', 'char *',
                                  'char *', 'int'),
        // most recent observed request rate
        request_rate_checked: this.dtp.addProbe('request_rate_checked', 'int'),
        // latency of the handled request, average request latency
        request_handled: this.dtp.addProbe('request_handled', 'char *',
                'char *', 'int'),
        // number of pending requestsm, request rate, url, method
        request_throttled: this.dtp.addProbe('request_throttled', 'int', 'int',
                'char *', 'char *'),
        // number of requests waiting for processing, number of requests
        // being processed
        queue_status: this.dtp.addProbe('queue_status', 'int', 'int')
    };
    this.dtp.enable();

    this.enabled = options.enabled;
    this.concurrency = options.concurrency;
    this.requestRateCapacity = options.requestRateCapacity;
    this.rateCheckInterval = options.rateCheckInterval;
    this.queueTolerance = options.queueTolerance;

    this.requestQueue = vasync.queue(function (task, callback) {
        task(callback);
    }, this.concurrency);

    this.lastCheck = process.hrtime();
    this.mostRecentRequestRate = 0.0;
    this.requestsPerCheckInterval = 0;

    this.requestsHandled = 0;
}

/*
 * Computes the observed request rate in the most recent check interval. This
 * function is called approximately every 'rateCheckInterval' seconds. The
 * returned figure has unit requests per second.
 */
Throttle.prototype.computeRequestRate = function computeRequestRate() {
    var checkTime = process.hrtime();
    var elapsed = jsprim.hrtimeDiff(checkTime, this.lastCheck);
    var elapsedSec = elapsed[1] / Math.pow(10, 9);
    if (elapsedSec === 0) {
        return (0);
    }
    this.lastCheck = checkTime;
    return (this.requestsPerCheckInterval / elapsedSec);
};

Throttle.prototype.computeRequestLatency = function computeRequestLatency(req) {
    assert.ok(req, 'req missing');
    assert.ok(req.throttleStartTime, 'throttle start time missing');

    var latency = process.hrtime(req.throttleStartTime);
    this.throttle_probes.request_handled.fire(function () {
        return ([req.method, req.url, latency[1]]);
    });
    return (latency);
};

Throttle.prototype.wait = function wait(req, res, next) {
    var self = this;

    self.requestsPerCheckInterval++;

    var elapsedSec = ((process.hrtime(self.lastCheck))[1]) / Math.pow(10, 9);
    if (elapsedSec > self.rateCheckInterval) {
        self.mostRecentRequestRate = self.computeRequestRate();
        self.requestsPerCheckInterval = 0;

        self.throttle_probes.request_rate_checked.fire(function () {
            return ([self.mostRecentRequestRate]);
        });
    }

    self.throttle_probes.request_received.fire(function () {
        return ([req.method, req.url, self.requestQueue.length()]);
    });

    if ((self.mostRecentRequestRate > self.requestRateCapacity) &&
            (self.requestQueue.length() >= self.queueTolerance)) {
        self.throttle_probes.request_throttled.fire(function () {
            return ([self.requestQueue.length(), self.mostRecentRequestRate,
                req.url, req.method]);
        });
        if (self.enabled) {
            next(new ThrottledError());
            return;
        }
    }

    // The restify server's 'after' event handler will invoke
    // computeRequestLatency, which uses this value to determine how long
    // it took to process a particular request - exposing statistic via dtrace
    // and making the figure available to muskie as well.
    req.throttleStartTime = process.hrtime();

    // The request only passes through the request queue if the throttle is
    // enabled. Otherwise, we invoke the work function immediately.
    if (self.enabled) {
        self.requestQueue.push(function (cb) {
            next();
            cb();
        });
    } else {
        next();
    }

    self.throttle_probes.queue_status.fire(function () {
        return ([self.requestQueue.queued.length, self.requestQueue.npending]);
    });
};
