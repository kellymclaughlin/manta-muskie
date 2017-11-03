/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var _helper = __dirname + '/helper.js';
if (require.cache[_helper])
    delete require.cache[_helper];
var helper = require(_helper);

var MemoryStream = require('stream').PassThrough;
var once = require('once');
var util = require('util');
var sprintf = util.format;
var vasync = require('vasync');

var afterEach = helper.afterEach;
var beforeEach = helper.beforeEach;
var helperTest = helper.test;

///--- Helpers

function writeObject(client, key, roles, cb) {
    cb = once(cb);
    var headers;
    if (typeof (roles) === 'function') {
        cb = roles;
    } else if (typeof (roles) === 'object') {
        headers = roles;
    } else {
        headers = {
            'role-tag': roles
        };
    }
    var input = new MemoryStream();
    var msg = JSON.stringify({hello: 'world'});
    var opts = {
        type: 'application/json',
        headers: headers,
        size: Buffer.byteLength(msg)
    };
    var output = client.createWriteStream(key, opts);
    output.once('close', cb.bind(null, null));
    output.once('error', cb);
    input.pipe(output);
    input.end(msg);
}


function addTag(client, key, tag, cb) {
    client.info(key, function (err, info) {
        if (err) {
            cb(err);
            return;
        }
        var tags = info.headers['role-tag'];
        if (tags) {
            /* JSSTYLED */
            tags = tags.split(/\s*,\s*/);
        } else {
            tags = [];
        }
        var index = tags.indexOf(tag);
        if (index < 0) {
            tags.push(tag);
        }
        client.chattr(key, {
            headers: {
                'role-tag': tags.join(',')
            }
        }, cb);
    });
}


function delTag(client, key, tag, cb) {
    client.info(key, function (err, info) {
        if (err) {
            cb(err);
            return;
        }
        var tags = info.headers['role-tag'] || '';
        if (tags) {
            /* JSSTYLED */
            tags = tags.split(/\s*,\s*/);
        } else {
            tags = [];
        }
        var index = tags.indexOf(tag);
         if (index >= 0) {
            tags.splice(index, 1);
        }
         client.chattr(key, {
            headers: {
                'role-tag': tags.join(',')
            }
        }, cb);
    });
}

function jobWait(client, jobId, cb) {
    client.job(jobId, function (err, res) {
        if (err) {
            cb(err);
            return;
        }
        if (res.state !== 'done') {
            setTimeout(jobWait.bind(null, client, jobId, cb), 2000);
            return;
        }
        client.jobErrors(jobId, function (err2, errors) {
            if (err2) {
                cb(err2);
                return;
            }
            var result = [];
            errors.on('err', function (e) {
                result.push(e);
            });
            errors.once('end', function () {
                cb(null, result);
            });
        });
    });
}

var clients = {};

///--- Setup
function before(cb) {
    clients.sdcClient = helper.createSDCClient();
    clients.client = helper.createClient();
    clients.jsonClient = helper.createJsonClient();
    clients.rawClient = helper.createRawClient();
    clients.userClient = helper.createUserClient('muskie_test_user');
    clients.paths = [];
    clients.jobs = [];

    cb();
}

///--- Teardown
function after(cb) {
    vasync.forEachParallel({
        func: clients.client.unlink.bind(clients.client),
        inputs: clients.paths
    }, function (err) {
        vasync.forEachParallel({
            func: clients.client.cancelJob.bind(clients.client),
            inputs: clients.jobs
        }, function (err2) {
            clients.client.close();
            clients.rawClient.close();
            clients.userClient.close();
            clients.sdcClient.client.close();

            cb();
        });
    });
}

///--- Tests

// var test = helperTest.bind(clients, before, after);
var test = beforeEach(helperTest, function _before(t) {
    before(function() {
        t.end();
    });
});
test = afterEach(test, function _after(t) {
    after(function() {
        t.end();
    });
});

test('default role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_default';

    t.plan(1);

    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            return;
        }
        clients.paths.push(path);
        clients.userClient.get(path, function (err2, res) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.ok(res);
        });
    });
});


test('inactive role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_limit';

    t.plan(1);

    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            return;
        }
        clients.paths.push(path);
        clients.userClient.get(path, function (err2) {
            if (!err2) {
                t.fail(err2, 'error expected');
                return;
            }
            t.equal(err2.name, 'NoMatchingRoleTagError');
        });
    });
});


test('assume non-default role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_limit';

    t.plan(1);

    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            return;
        }
        clients.paths.push(path);
        clients.userClient.get(path, {
            headers: {
                'role': 'muskie_test_role_limit'
            }
        }, function (err2, res) {
            if (err2) {
                t.fail(err2);
                return;
            }

            t.ok(res);
        });
    });
});


test('assume multiple roles', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_limit';

    t.plan(1);

    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            return;
        }
        clients.paths.push(path);
        clients.userClient.get(path, {
            headers: {
                'role': 'muskie_test_role_default,muskie_test_role_limit'
            }
        }, function (err2, res) {
            if (err2) {
                t.fail(err2);
                return;
            }

            t.ok(res);
        });
    });
});


test('assume wrong role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_default';
    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        clients.paths.push(path);
        clients.userClient.get(path, {
            headers: {
                'role': 'muskie_test_role_limit'
            }
        }, function (err2) {
            if (!err2) {
                t.fail(err2, 'error expected');
                t.end();
                return;
            }
            t.equal(err2.name, 'NoMatchingRoleTagError');
            t.end();
        });
    });
});


test('assume limit roles (*)', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_limit';
    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        clients.paths.push(path);
        clients.userClient.get(path, {
            headers: {
                'role': '*'
            }
        }, function (err2, res) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.ok(res);
            t.end();
        });
    });
});


test('assume bad role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_other';
    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        clients.paths.push(path);
        clients.userClient.get(path, {
            headers: {
                'role': 'muskie_test_role_other'
            }
        }, function (err2) {
            if (!err2) {
                t.fail(err2, 'error expected');
                t.end();
                return;
            }
            t.equal(err2.name, 'InvalidRoleError');
            t.end();
        });
    });
});


test('mchmod', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_write';

    t.plan(1);

    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }

        clients.paths.push(path);
        clients.userClient.chattr(path, {
            headers: {
                'role': 'muskie_test_role_write',
                'role-tag': 'muskie_test_role_other'
            }
        }, function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            clients.client.info(path, function (err3, info) {
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }
                t.equal(info.headers['role-tag'], 'muskie_test_role_other');
                t.end();
            });
        });
    });
});


test('mchmod bad role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_write';

    t.plan(1);

    writeObject(clients.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }

        clients.paths.push(path);
        clients.userClient.chattr(path, {
            headers: {
                'role': 'muskie_test_role_write',
                'role-tag': 'asdf'
            }
        }, function (err2) {
            if (!err2) {
                t.fail('error expected');
                t.end();
                return;
            }
            t.equal(err2.name, 'InvalidRoleTagError');
            t.end();
        });
    });
});


test('created object gets roles', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var dir = sprintf('/%s/stor', clients.client.user);

    addTag(clients.client, dir, 'muskie_test_role_write', function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        writeObject(clients.userClient, path, {
            'role': 'muskie_test_role_write'
        }, function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            clients.paths.push(path);

            clients.client.info(path, function (err3, info) {
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                /* JSSTYLED */
                var tags = info.headers['role-tag'].split(/\s*,\s*/);
                t.ok(tags.indexOf('muskie_test_role_write') >= 0);

                delTag(clients.client, dir, 'muskie_test_role_write',
                        function (err4) {

                    if (err4) {
                        t.fail(err4);
                        t.end();
                        return;
                    }
                    t.end();
                });
            });
        });
    });
});


test('create object parent directory check', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    // t.plan(2);

    clients.client.info(path, function (err3, info) {
        if (!err3) {
            t.fail('test object already exists');
            t.end();
            return;
        }
        writeObject(clients.userClient, path, {
            'role': 'muskie_test_role_write'
        }, function (err2) {
               if (!err2) {
                   clients.paths.push(path);
                   t.fail('expected error');
                   t.end();
                   return;
               }

               t.equal(err2.name, 'NoMatchingRoleTagError');
               t.end();
           });
    });
});


test('create object parent directory check', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);

    t.plan(1);

    writeObject(clients.userClient, path, {
        'role': 'muskie_test_role_write'
    }, function (err) {
        if (!err) {
            clients.paths.push(path);
            t.fail('expected error');
            t.end();
            return;
        }

        t.equal(err.name, 'NoMatchingRoleTagError');
        t.end();
    });
});


test('create directory parent directory check', function (t) {
    var path = sprintf('/%s/stor/muskie_test_dir', clients.client.user);

    t.plan(1);

    clients.userClient.mkdir(path, {
        headers: {
            'role': 'muskie_test_role_write'
        }
    }, function (err2) {
        if (!err2) {
            clients.paths.push(path);
            t.fail('expected error');
            t.end();
            return;
        }

        t.equal(err2.name, 'NoMatchingRoleTagError');
        t.end();
    });
});


// Ideally, getting a nonexistent object should mean a check on the parent
// directory to see if the user has read permissions on the directory. However,
// since this requires an additional lookup, we're just returning 404s for now.
test('get nonexistent object 404', function (t) {
    var path = sprintf('/%s/stor/muskie_test_dir', clients.client.user);
    clients.client.get(path, function (err2) {
        if (!err2) {
            t.fail('error expected');
            t.end();
            return;
        }
        t.equal(err2.name, 'ResourceNotFoundError');
        t.end();
    });
});


test('signed URL uses default roles', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_default';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(clients.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: clients.userClient
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            clients.jsonClient.get({
                path: signed
            }, function (err, req, res, obj) {
                if (err) {
                    t.fail(err);
                    cb(err);
                    return;
                }
                t.ok(obj);
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('signed URL ignores role headers', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_limit';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(clients.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: clients.userClient
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            clients.jsonClient.get({
                path: signed,
                headers: {
                    role: 'muskie_test_role_limit'
                }
            }, function (err) {
                if (!err) {
                    t.fail('expected error');
                    cb();
                    return;
                }

                t.equal(err.name, 'NoMatchingRoleTagError');
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('signed URL with included role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_limit';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(clients.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: clients.userClient,
                role: [ 'muskie_test_role_limit' ]
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            clients.jsonClient.get({
                path: signed
            }, function (err, req, res, obj) {
                if (err) {
                    t.fail(err);
                    cb(err);
                    return;
                }
                t.ok(obj);
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('signed URL with included wrong role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_default';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(clients.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: clients.userClient,
                role: [ 'muskie_test_role_limit' ]
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            clients.jsonClient.get({
                path: signed
            }, function (err) {
                if (!err) {
                    t.fail('expected error');
                    cb();
                    return;
                }

                t.equal(err.name, 'NoMatchingRoleTagError');
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('signed URL with included invalid role', function (t) {
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var roles = 'muskie_test_role_default';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(clients.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: clients.userClient,
                role: [ 'muskie_test_role_asdfasdf' ]
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            clients.jsonClient.get({
                path: signed
            }, function (err) {
                if (!err) {
                    t.fail('expected error');
                    cb();
                    return;
                }

                t.equal(err.name, 'InvalidRoleError');
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('create job ACL check failure', function (t) {
    clients.userClient.createJob({
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    }, function (err, jobId) {
        if (jobId) {
            clients.jobs.push(jobId);
        }

        if (!err) {
            t.fail('error expected');
            t.end();
            return;
        }

        t.equal(err.name, 'NoMatchingRoleTagError');
        t.end();
    });
});


test('create job ACL check success', function (t) {
    var path = sprintf('/%s/jobs', clients.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };
    addTag(clients.client, path, 'muskie_test_role_jobs', function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }

        clients.userClient.createJob(job, {
            headers: {
                role: 'muskie_test_role_jobs'
            }
        }, function (err2, jobId) {
            if (jobId) {
                clients.jobs.push(jobId);
            }

            delTag(clients.client, path, 'muskie_test_role_jobs',
                    function (err3) {

                if (err2 || err3) {
                    t.fail(err2 || err3);
                    t.end();
                    return;
                }

                t.end();
            });
        });
    });
});


test('job inputs - no managejob on /jobs', function (t) {
    var jobRoot = sprintf('/%s/jobs', clients.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };
    writeObject(clients.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        clients.paths.push(path);

        addTag(clients.client, jobRoot, 'muskie_test_role_create_job',
                function (err2) {

            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            clients.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_create_job'
                }
            }, function (err3, jobId) {
                if (jobId) {
                    clients.jobs.push(jobId);
                }
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                clients.userClient.addJobKey(jobId, path, function (err4) {
                    if (!err4) {
                        t.fail('expected error');
                    } else {
                        t.equal(err4.name, 'NoMatchingRoleTagError');
                    }

                    delTag(clients.client, path, 'muskie_test_role_create_job',
                            function (err5) {

                        if (err5) {
                            t.fail(err5);
                            t.end();
                            return;
                        }

                        t.end();
                    });
                });
            });
        });
    });
});


test('job inputs - no managejob active', function (t) {
    var jobRoot = sprintf('/%s/jobs', clients.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };
    writeObject(clients.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        clients.paths.push(path);

        addTag(clients.client, jobRoot, 'muskie_test_role_jobs', function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            clients.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs'
                }
            }, function (err3, jobId) {
                if (jobId) {
                    clients.jobs.push(jobId);
                }
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                clients.userClient.addJobKey(jobId, path, function (err4) {
                    if (!err4) {
                        t.fail('expected error');
                    } else {
                        t.equal(err4.name, 'NoMatchingRoleTagError');
                    }

                    delTag(clients.client, path, 'muskie_test_role_jobs',
                            function (err5) {

                        if (err5) {
                            t.fail(err5);
                            t.end();
                            return;
                        }

                        t.end();
                    });
                });
            });
        });
    });
});


test('job inputs - no getobject on input key', function (t) {
    var jobRoot = sprintf('/%s/jobs', clients.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };

    writeObject(clients.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        clients.paths.push(path);

        addTag(clients.client, jobRoot, 'muskie_test_role_jobs', function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            clients.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs'
                }
            }, function (err3, jobId) {
                if (jobId) {
                    clients.jobs.push(jobId);
                }
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                clients.userClient.addJobKey(jobId, path, {
                    headers: {
                        role: 'muskie_test_role_jobs'
                    }
                }, function (err4) {
                    delTag(clients.client, jobRoot, 'muskie_test_role_jobs',
                            function (err5) {

                        if (err4 || err5) {
                            t.fail(err4 || err5);
                            t.end();
                            return;
                        }

                        function checkJob() {
                            clients.client.job(jobId, function (err7, res) {
                                if (err7) {
                                    t.fail(err7);
                                    t.end();
                                    return;
                                }

                                if (res.state !== 'done') {
                                    setTimeout(checkJob, 2000);
                                    return;
                                }

                                clients.client.jobErrors(jobId,
                                        function (err8, errors) {

                                    if (err8) {
                                        t.fail(err8);
                                        t.end();
                                        return;
                                    }

                                    var list = [];

                                    errors.on('err', function (e) {
                                        list.push(e);
                                    });

                                    errors.once('end', function () {
                                        t.equal(list.length, 1);
                                        if (list.length !== 1) {
                                            t.end();
                                            return;
                                        }
                                        t.equal(list[0].code,
                                            'AuthorizationError');
                                        t.end();
                                    });
                                });
                            });
                        }

                        clients.client.endJob(jobId, function (err6) {
                            if (err6) {
                                t.fail(err6);
                                t.end();
                                return;
                            }
                            clients.jobs.pop();
                            setTimeout(checkJob, 2000);
                        });
                    });
                });
            });
        });
    });
});


test('job inputs - context change after job creation', function (t) {
    var jobRoot = sprintf('/%s/jobs', clients.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };
    var jobId;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(clients.client, path, 'muskie_test_role_default',
                    function (err) {

                if (err) {
                    cb(err);
                    return;
                }
                clients.paths.push(path);
                cb();
            });
        },
        function tag(_, cb) {
            addTag(clients.client, jobRoot, 'muskie_test_role_jobs_only', cb);
        },
        function create(_, cb) {
            clients.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs_only, ' +
                          'muskie_test_role_fromjob, ' +
                          'muskie_test_role_default'
                }
            }, function (err, id) {
                if (err) {
                    cb(err);
                    return;
                }

                jobId = id;
                cb();
            });
        },
        function input(_, cb) {
            clients.userClient.addJobKey(jobId, path, {
                headers: {
                    role: 'muskie_test_role_jobs_only'
                }
            }, cb);
        },
        function end(_, cb) {
            clients.client.endJob(jobId, cb);
        },
        function check(_, cb) {
            jobWait(clients.client, jobId, function (err, errors) {
                if (err) {
                    cb(err);
                    return;
                }
                if (errors.length < 1) {
                    t.fail('error expected');
                    cb();
                    return;
                }
                t.equal(errors[0].code, 'InternalError');
                cb();
            });
        }
    ]}, function (err, results) {
        delTag(clients.client, jobRoot, 'muskie_test_role_jobs_only',
                function (err2) {

            if (err) {
                t.fail(results.operations[results.ndone - 1]);
                t.end();
                return;
            }

            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.end();
        });
    });
});


test('no putdirectory on job creation', function (t) {
    var jobRoot = sprintf('/%s/jobs', clients.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };

    addTag(clients.client, jobRoot, 'muskie_test_role_jobs_only', function (err2) {
        if (err2) {
            t.fail(err2);
            t.end();
            return;
        }

        clients.userClient.createJob(job, {
            headers: {
                role: 'muskie_test_role_jobs_only'
            }
        }, function (err3, jobId) {
            if (jobId) {
                clients.jobs.push(jobId);
            }

            delTag(clients.client, jobRoot, 'muskie_test_role_jobs_only',
                    function (err4) {

                if (!err3) {
                    t.fail('expected error');
                    t.end();
                    return;
                }

                if (err4) {
                    t.fail(err4);
                    t.end();
                    return;
                }

                t.equal(err3.restCode, 'MissingPermission');
                t.end();
            });
        });
    });
});


test('job OK', function (t) {
    var jobRoot = sprintf('/%s/jobs', clients.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };

    writeObject(clients.client, path, 'muskie_test_role_default', function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        clients.paths.push(path);

        addTag(clients.client, jobRoot, 'muskie_test_role_jobs', function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            clients.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs, ' +
                            'muskie_test_role_default, ' +
                            'muskie_test_role_write'
                }
            }, function (err3, jobId) {
                if (jobId) {
                    clients.jobs.push(jobId);
                }
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                clients.userClient.addJobKey(jobId, path, {
                    headers: {
                        role: 'muskie_test_role_jobs'
                    }
                }, function (err4) {
                    delTag(clients.client, jobRoot, 'muskie_test_role_jobs',
                            function (err5) {

                        if (err4 || err5) {
                            t.fail(err4 || err5);
                            t.end();
                            return;
                        }

                        function checkJob() {
                            clients.client.job(jobId, function (err7, res) {
                                if (err7) {
                                    t.fail(err7);
                                    t.end();
                                    return;
                                }

                                if (res.state !== 'done') {
                                    setTimeout(checkJob, 2000);
                                    return;
                                }

                                clients.client.jobErrors(jobId,
                                        function (err8, errors) {

                                    if (err8) {
                                        t.fail(err8);
                                        t.end();
                                        return;
                                    }

                                    var list = [];

                                    errors.on('err', function (e) {
                                        list.push(e);
                                    });

                                    errors.once('end', function () {
                                        if (list.length > 0) {
                                            t.fail(list[0]);
                                            t.end();
                                            return;
                                        }
                                        t.equal(list.length, 0);
                                        t.end();
                                    });
                                });
                            });
                        }

                        clients.client.endJob(jobId, function (err6) {
                            if (err6) {
                                t.fail(err6);
                                t.end();
                                return;
                            }
                            clients.jobs.pop();
                            setTimeout(checkJob, 2000);
                        });
                    });
                });
            });
        });
    });
});


test('assets - no getobject on asset', function (t) {
    var jobRoot = sprintf('/%s/jobs', clients.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', clients.client.user);
    var asset = sprintf('/%s/stor/muskie_test_aasset', clients.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            assets: [ asset ],
            exec: 'wc'
        } ]
    };
    var jobId;

    vasync.pipeline({funcs: [
        function writeobj(_, cb) {
            writeObject(clients.client, path, 'muskie_test_role_default',
                    function (err) {

                if (err) {
                    cb(err);
                    return;
                }
                clients.paths.push(path);
                cb();
            });
        },
        function writeasset(_, cb) {
            writeObject(clients.client, asset, function (err) {
                if (err) {
                    cb(err);
                    return;
                }
                clients.paths.push(asset);
                cb();
            });
        },
        function tag(_, cb) {
            addTag(clients.client, jobRoot, 'muskie_test_role_jobs', cb);
        },
        function create(_, cb) {
            clients.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs'
                }
            }, function (err, id) {
                if (err) {
                    cb(err);
                    return;
                }

                jobId = id;
                cb();
            });
        },
        function input(_, cb) {
            clients.userClient.addJobKey(jobId, path, {
                headers: {
                    role: 'muskie_test_role_jobs'
                }
            }, cb);
        },
        function end(_, cb) {
            clients.client.endJob(jobId, cb);
        },
        function check(_, cb) {
            jobWait(clients.client, jobId, function (err, errors) {
                if (err) {
                    cb(err);
                    return;
                }
                if (errors.length < 1) {
                    t.fail('error expected');
                    cb();
                    return;
                }
                t.equal(errors[0].code, 'AuthorizationError');
                cb();
            });
        }
    ]}, function (err, results) {
        delTag(clients.client, jobRoot, 'muskie_test_role_jobs',
                function (err2) {

            if (err) {
                t.fail(results.operations[results.ndone - 1]);
                t.end();
                return;
            }

            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.end();
        });
    });
});

// TODO assets OK

// TODO conditions - overwrite

// TODO conditions - day/date/time

// TODO conditions - sourceip

// TODO conditions - user-agent
