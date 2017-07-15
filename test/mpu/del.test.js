/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var fs = require('fs');

var manta = require('manta');
var path = require('path');
var sshpk = require('sshpk');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

if (require.cache[path.join(__dirname, '/../helper.js')])
    delete require.cache[path.join(__dirname, '/../helper.js')];
if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var testHelper = require('../helper.js');
var helper = require('./helper.js');

var after = testHelper.after;
var before = testHelper.before;
var test = testHelper.test;

var ifErr = helper.ifErr;

/*
 * We need an operator account for these tests, so we use poseidon, unless an
 * alternate one is provide.
 */
var TEST_OPERATOR = 'poseidon' || process.env.MANTA_OPERATOR_USER;
var TEST_OPERATOR_KEY = (process.env.HOME + '/.ssh/id_rsa_poseidon') ||
                         process.env.MANTA_OPERATOR_KEYFILE;

before(function (cb) {
    helper.initMPUTester.call(this, cb);
});


after(function (cb) {
    helper.cleanupMPUTester.call(this, cb);
});

/*
 * Helper to create a Manta client for the operator account.
 *
 * Parameters:
 *  - user: the operator account
 *  - keyFile: local path to the private key for this account
 */
function createOperatorClient(user, keyFile) {
    var key = fs.readFileSync(keyFile);
    var keyId = sshpk.parseKey(key, 'auto').fingerprint('md5').toString();

    var log = testHelper.createLogger();
    var client = manta.createClient({
        agent: false,
        connectTimeout: 2000,
        log: log,
        retry: false,
        sign: manta.privateKeySigner({
            key: key,
            keyId: keyId,
            log: log,
            user: user
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL || 'http://localhost:8080',
        user: user
    });

    return (client);
}


// Delete parts/upload directories: allowed cases

test('del upload directory with operator override', function (t) {
    var self = this;

    self.client.close();
    self.client = createOperatorClient(TEST_OPERATOR, TEST_OPERATOR_KEY);

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            query: {
                override: true
            }
        };
        self.client.unlink(self.uploadPath(), opts, function (err2, res) {
            if (ifErr(t, err2, 'unlink')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('del part with operator override', function (t) {
    var self = this;

    self.client.close();
    self.client = createOperatorClient(TEST_OPERATOR, TEST_OPERATOR_KEY);

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err, 'uploaded part')) {
                t.end();
                return;
            }

            var opts = {
                query: {
                    override: true
                }
            };
            self.client.unlink(self.uploadPath(pn), opts, function (err3, res) {
                if (ifErr(t, err3, 'unlink')) {
                    t.end();
                    return;
                }

                t.ok(res);
                t.checkResponse(res, 204);
                t.end();
            });
        });
    });
});


// Delete parts/upload directories: operator, no override provided

test('del upload directory: operator but no override', function (t) {
    var self = this;

    self.client.close();
    self.client = createOperatorClient(TEST_OPERATOR, TEST_OPERATOR_KEY);

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.client.unlink(self.uploadPath(), function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2,
                'UnprocessableEntityError'), err2);
            t.checkResponse(res, 422);
            t.end();
        });
    });
});


test('del part: operator but no override', function (t) {
    var self = this;

    self.client.close();
    self.client = createOperatorClient(TEST_OPERATOR, TEST_OPERATOR_KEY);

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err, 'uploaded part')) {
                t.end();
                return;
            }

            self.client.unlink(self.uploadPath(pn), function (err3, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.ok(verror.hasCauseWithName(err3,
                    'UnprocessableEntityError'), err3);
                t.checkResponse(res, 422);
                t.end();
            });
        });
    });
});


// Delete parts/upload directories: non-operator, override provided

test('del upload directory: non-operator with override', function (t) {
    var self = this;
    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            query: {
                override: true
            }
        };

        self.client.unlink(self.uploadPath(), opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2,
                'MethodNotAllowedError'), err2);
            t.checkResponse(res, 405);
            t.end();
        });
    });
});


test('del part: non-operator with override', function (t) {
    var self = this;
    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            var opts = {
                query: {
                    override: true
                }
            };

            self.client.unlink(self.uploadPath(pn), opts, function (err3, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.ok(verror.hasCauseWithName(err3,
                    'MethodNotAllowedError'), err3);
                t.checkResponse(res, 405);
                t.end();
            });
        });
    });
});
