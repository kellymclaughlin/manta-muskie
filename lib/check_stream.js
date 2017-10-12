/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * CheckStream calculates the md5 sum of a data stream. In practice this is
 * used to calculate the md5 sum of objects as they are streamed to or from
 * sharks.
 *
 * As the name implies, this is implemented as a pass-through stream. As chunks
 * of bytes flow through the _write function of the CheckStream, a running md5
 * sum is computed. The final md5 sum can be retrieved using
 * CheckStream.digest().
 *
 * If CheckStream goes too long without receiving input, the stream emits a
 * 'timeout' event. This is used elsewhere to abandon the stream by calling
 * CheckStream.abandon().
 *
 * CheckStream ensures that the number of bytes streamed doesn't exceed
 * what is expected. The maximum bytes CheckStream will read is set by the
 * 'maxBytes' argument to the constructor.
 *
 * Throughput metrics are collected in the CheckStream. Depending on the
 * value of the 'inbound' flag, either inbound or outbound throughput is
 * tracked.
 *
 */

var crypto = require('crypto');
var stream = require('stream');
var util = require('util');

var assert = require('assert-plus');

require('./errors');


///--- Helpers

function onTimeoutHandler() {
    this.emit('timeout');
}



///--- API

function CheckStream(opts) {
    assert.object(opts, 'options');
    assert.optionalString(opts.algorithm, 'options.algorithm');
    assert.number(opts.maxBytes, 'options.maxBytes');
    assert.number(opts.timeout, 'opts.timeout');
    assert.object(opts.collector, 'opts.collector');
    assert.bool(opts.inbound, 'opts.inbound');

    stream.Writable.call(this, opts);

    var self = this;

    this.algorithm = opts.algorithm || 'md5';
    this.bytes = 0;
    this.hash = crypto.createHash(this.algorithm);
    this.maxBytes = opts.maxBytes;
    this.start = Date.now();
    this.timeout = opts.timeout;
    this.timer = setTimeout(onTimeoutHandler.bind(this), this.timeout);

    if (this.inbound) {
        this.counter = opts.collector.counter({
            name: 'muskie_received_bytes',
            help: 'count of bytes streamed into muskie'
        });
    } else {
        this.counter = opts.collector.counter({
            name: 'muskie_sent_bytes',
            help: 'count of bytes streamed out of muskie'
        });
    }

    this.once('finish', function onFinish() {
        setImmediate(function () {
            if (!self._digest)
                self._digest = self.hash.digest('buffer');

            self.emit('done');
        });
    });
}
util.inherits(CheckStream, stream.Writable);
module.exports = CheckStream;


CheckStream.prototype.abandon = function abandon() {
    this._dead = true;
    clearTimeout(this.timer);
    this.removeAllListeners('error');
    this.removeAllListeners('finish');
    this.removeAllListeners('length_exceeded');
    this.removeAllListeners('timeout');
};


CheckStream.prototype.digest = function digest(encoding) {
    assert.optionalString(encoding, 'encoding');

    clearTimeout(this.timer);

    if (!this._digest)
        this._digest = this.hash.digest('buffer');

    var ret = this._digest;
    if (this._digest && encoding)
        ret = this._digest.toString(encoding);

    return (ret);
};


CheckStream.prototype._write = function _write(chunk, encoding, cb) {
    if (this._dead) {
        cb();
        return;
    }
    var self = this;

    clearTimeout(this.timer);
    this.hash.update(chunk, encoding);
    this.bytes += chunk.length;
    this.counter.add(chunk.length);
    if (this.bytes > this.maxBytes) {
        this.emit('length_exceeded', this.bytes);
        setImmediate(function () {
            cb(self._dead ? null : new MaxSizeExceededError(self.maxBytes));
        });
    } else {
        this.timer = setTimeout(onTimeoutHandler.bind(this), this.timeout);
        cb();
    }
};


CheckStream.prototype.toString = function toString() {
    return ('[object CheckStream<' + this.algorithm + '>]');
};
