import _ from 'lodash';
import convict from 'convict';
import bluebird from 'bluebird';
import redis from 'redis';

bluebird.promisifyAll(redis);

import jobs from '../jobs';

const jobId = Number(process.argv[2]);
const chunkId = Number(process.argv[3]);

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

// TODO: add logging to file
// const logger = bunyan.createLogger({
//   name: `(mapper, job ${jobId} chunk ${chunkId}) node-map-reduce@${config.get('env')}`
// });

async function run () {
  let redisClient;

  let job = _.find(jobs, { id: jobId });

  // Note, we aren't configuring the app to handle dropped redis connections.
  // In production environments this can happen without it being indicative of a problem,
  // so any adaptation of this code for a prod environment needs to account for this.
  await new Promise((resolve, reject) => {
    redisClient = redis.createClient(config.get('redis_port'), config.get('redis_uri'));

    redisClient.on('connect', () => resolve());
  });

  const rawPayload = await redisClient.getAsync(`mapreduce:${jobId}:${chunkId}:payload`);
  const payload = JSON.parse(rawPayload);
  const results = _.map(payload, job.mapper);

  await redisClient.setAsync(`mapreduce:${jobId}:${chunkId}:results`, JSON.stringify(results));
}

run()
  .then(() => {
    process.send(`${jobId}:${chunkId} COMPLETE`, () => {
      process.exit(0);
    });
  });
