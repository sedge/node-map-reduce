import http from 'http';

import _ from 'lodash';

import bunyan from 'bunyan';
import request from 'supertest';
import bluebird from 'bluebird';
import redis from 'redis';
import convict from 'convict';

import chai from 'chai';
import chaiAsPromised from 'chai-as-promised';

chai.use(chaiAsPromised);

bluebird.promisifyAll(redis);

import jobs from './jobs';

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

import { constructServer } from './app';

const logger = bunyan.createLogger({
  name: `(port ${config.get('app_port')}) node-map-reduce@${config.get('env')}`
});

const {
	expect
} = chai;

describe('[Integration tests]', async function() {
	before(async function() {
		this.timeout(10000);

		await new Promise((resolve, reject) => {
		  this.redisClient = redis.createClient(config.get('redis_port'), config.get('redis_uri'));

		  this.redisClient.on('connect', () => resolve());
		});

		this.redisClient.on('error', err => {
		  logger.error(`REDIS error`);
		  logger.error(err);

		  throw err;
		});

		this.app = constructServer({ redisClient: this.redisClient, logger, config });
	})

	after(async function() {
		await this.redisClient.quitAsync();
	});

	describe('Job: @transactionCount', async function() {
		this.timeout(10000);

		const job = jobs[0];

		it('should process the job correctly', async function () {
			const testConfig = {
				payload: [
					{id:1, merchant: 'Starbucks', amount:1.78, date: '2018-01-01', reflected: 'GOOD', user_id: 1},
					{id:2, merchant: 'Starbucks', amount:5.76, date: '2018-01-02', reflected: 'GOOD', user_id: 1},
					{id:3, merchant: 'Tim Hortons', amount:8.76, date: '2018-01-03', reflected: 'NEUTRAL', user_id: 1},
					{id:4, merchant: 'Tim Hortons', amount:5.67, date: '2018-01-04', reflected: 'BAD', user_id: 1},
					{id:5, merchant: 'Tim Hortons', amount:11.76, date: '2018-01-06', reflected: 'GOOD', user_id: 1},
					{id:5, merchant: 'Starbucks', amount:12.36, date: '2018-01-07', reflected: 'GOOD', user_id: 2},
					{id:5, merchant: 'Tim Hortons', amount:1.45, date: '2018-01-08', reflected: 'GOOD', user_id: 2}
				],
				expectedResults: {
					merchant_transaction_count: [
						['Starbucks', 3],
						['Tim Hortons', 4]
					]
				}
			};

			const response = await request(this.app)
				.post(job.endpoint)
				.send(testConfig.payload)
				.expect('Content-Type', /json/)
				.expect(200);

			return expect(response.body).to.eql(testConfig.expectedResults);
		});
	});

	describe('Job: @happinessPercentagePerUser', async function() {
		this.timeout(10000);

		const job = jobs[1];

		it('should process the job correctly', async function () {
			const testConfig = {
				payload: [
					{id:1, merchant: 'Starbucks', amount:1.78, date: '2018-01-01', reflected: 'GOOD', user_id: 1},
					{id:2, merchant: 'Starbucks', amount:5.76, date: '2018-01-02', reflected: 'GOOD', user_id: 1},
					{id:3, merchant: 'Tim Hortons', amount:8.76, date: '2018-01-03', reflected: 'NEUTRAL', user_id: 1},
					{id:4, merchant: 'Tim Hortons', amount:5.67, date: '2018-01-04', reflected: 'BAD', user_id: 1},
					{id:5, merchant: 'Tim Hortons', amount:11.76, date: '2018-01-06', reflected: 'GOOD', user_id: 1},
					{id:5, merchant: 'Starbucks', amount:12.36, date: '2018-01-07', reflected: 'GOOD', user_id: 2},
					{id:5, merchant: 'Tim Hortons', amount:1.45, date: '2018-01-08', reflected: 'GOOD', user_id: 2},
					{id:1, name: 'John'},
					{id:2, name: 'Luke'}
				],
				expectedResults: {
					user_happiness: [
						['John', '60%'],
						['Luke', '100%']
					]
				}
			};

			const response = await request(this.app)
				.post(job.endpoint)
				.send(testConfig.payload)
				.expect('Content-Type', /json/)
				.expect(200);

			return expect(response.body).to.eql(testConfig.expectedResults);
		});
	});
});
