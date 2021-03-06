import _ from 'lodash';
import bunyan from 'bunyan';
import convict from 'convict';
import bluebird from 'bluebird';
import redis from 'redis';
import Redlock from 'redlock';

import { constructServer } from './app';

bluebird.promisifyAll(redis);

// TODO: Move this to it's own file to reduce redundant code
const config = convict({
  env: {
    doc: 'The application environment.',
    format: ['production', 'development', 'test'],
    default: 'development',
    env: 'NODE_ENV'
  },
  redis_uri: {
    doc: 'Redis queue cluster URI',
    format: 'ipaddress',
    default: '127.0.0.1',
    env: 'REDIS_URI'
  },
  redis_port: {
    doc: 'Redis queue cluster port',
    format: 'int',
    default: 6379,
    env: 'REDIS_PORT'
  },
  app_port: {
    doc: 'Port for the client-facing express server',
    format: 'int',
    default: 4000,
    env: 'REDIS_URI'
  },
  redis_drift_factor: {
    doc: 'Expected clock drift (see http://redis.io/topics/distlock)',
    format: 'Number',
    default: 0.01,
    env: 'REDIS_DRIFT_FACTOR'
  },
  redis_retry_count: {
    doc: 'Max lock retries before erroring',
    format: 'Number',
    default: 10,
    env: 'REDIS_RETRY_COUNT'
  },
  redis_retry_delay: {
    doc: 'Time in MS between lock attempts',
    format: 'Number',
    default: 200,
    env: 'REDIS_RETRY_DELAY'
  },
  redis_jitter: {
    doc: 'Performance boost. See: https://www.awsarchitectureblog.com/2015/03/backoff.html',
    format: 'Number',
    default: 200,
    env: 'REDIS_JITTER'
  },
  engine_chunk_size: {
    doc: 'Max number of records per discrete operation',
    format: 'int',
    default: 100,
    env: 'ENGINE_CHUNK_SIZE'
  },
});

config.loadFile(`./config.${config.get('env')}.json`);
config.validate({ allowed: 'strict' });

const logger = bunyan.createLogger({
  name: `(port ${config.get('app_port')}) node-map-reduce@${config.get('env')}`
});

async function init() {
  logger.info('************  INITIALIZING  ************');

  // Note, we aren't configuring the app to handle dropped redis connections.
  // In production environments this can happen without it being indicative of a problem,
  // so any adaptation of this code for a prod environment needs to account for this.
  let redisClient;

  await new Promise((resolve, reject) => {
    redisClient = redis.createClient(config.get('redis_port'), config.get('redis_uri'));

    redisClient.on('connect', () => resolve());
  });

  logger.info('************  REDIS CONNECTED  ************');

  redisClient.on('error', err => {
    logger.error(`REDIS error`);
    logger.error(err);

    throw err;
  });

  const redlock = new Redlock(
    [redisClient],
    {
      driftFactor: config.get('redis_drift_factor'),
      retryCount: config.get('redis_retry_count'),
      retryDelay: config.get('redis_retry_delay'),
      retryJitter: config.get('redis_jitter')
    }
  );

  const app = constructServer({ redisClient, redlock, logger, config });

  await new Promise((resolve, reject) => {
    app.listen(config.get('app_port'), err => {
      if (err) { reject(err); }

      resolve();
    })
  });

  logger.info('************  API SERVER CONNECTED  ************');
}

init()
  .then(
    () => logger.info('************ ALL SYSTEMS GO ************'),
    err => {
      logger.fatal('Mayday!');
      logger.fatal(err);

      process.exit(1);
    }
  );
