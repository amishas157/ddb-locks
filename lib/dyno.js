'use strict';
const keepalive = require('agentkeepalive');
const Dyno = require('@mapbox/dyno');

function getClient(table) {
  return Dyno({
    table: table,
    region: 'us-east-1',
    maxRetries: 5,
    httpOptions: {
      agent: new keepalive.HttpsAgent({
        maxSockets: Math.ceil(require('os').cpus().length * 16),
        keepAliveTimeout: 60000,
        keepAlive: true
      })
    }
  });
}

exports.getClient = getClient;
