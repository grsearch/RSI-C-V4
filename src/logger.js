'use strict';
// src/logger.js
const winston = require('winston');

const logger = winston.createLogger({
  level : process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'HH:mm:ss.SSS' }),
    winston.format.splat(),  // ★ 支持 logger.info('%s %d', a, b) 风格的占位符替换
    winston.format.printf(({ timestamp, level, message }) =>
      `${timestamp} [${level.toUpperCase()}] ${message}`)
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({
      filename: 'logs/app.log',
      maxsize : 10 * 1024 * 1024,
      maxFiles: 3,
    }),
  ],
});

module.exports = logger;
