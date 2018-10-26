import _ from 'lodash';
import bunyan from 'bunyan';
import convict from 'convict';
import bluebird from 'bluebird';
import redis from 'redis';

import { constructServer } from './app';

bluebird.promisifyAll(redis);

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

	const app = constructServer({ redisClient, logger, config });

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
