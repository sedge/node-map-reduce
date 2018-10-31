import _ from 'lodash';

import express from 'express';
import loggerMiddleware from 'morgan';
import cookieParser from 'cookie-parser';
import bodyParser from 'body-parser';

import { buildHandler } from './handler';
import jobs from './jobs';

export function constructServer ({ redisClient, redlock, logger, config }) {
  const app = express();

  app.disable('x-powered-by');

  app.use(loggerMiddleware('dev'));
  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({ extended: false }));
  app.use((req, res, next) => {
    res.set({
        'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE',
        'Access-Control-Expose-Headers': 'X-API-Version, Etag',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': [
            'Content-Type',
            'Content-Range',
            'Content-Disposition',
            'Origin',
            'Accept',
            'Authorization',
        ].join(', '),
    });
    next();
  });

  for (let job of jobs) {
    app.use(
      buildHandler({ job, redisClient, redlock, logger, config })
    );
  }

  return app;
}
