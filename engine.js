import cp from 'child_process';

import Bluebird from 'bluebird';
import _ from 'lodash';

// A quick note!
//
// This is a somewhat naive approach to performance, chosen because
// processing huge amounts of data through HTTP dumps is fairly unrealistic to begin with.
//
// A better solution would use a dedicated mapreduce technology like Spark or Hadoop
// and read the data to be processed from a database specialized in holding vast
// quantities of raw information, such as HDFS.
//
// Additionally, my chosen approach is complicated by the fact that nodejs doesn't
// handle concurrent processing very well. A better alternative would be a
// language with strong concurrency features, such as golang.
//
// Drawbacks to my approach include:
  // 1. Each "batch" of map or reduce operations (up to the configurable maximum) must finish
  //    before the next batch may begin, as opposed to a more sophisticated
  //    solution that might keep all available threads occupied at all times
  // 2. Threads aren't shared between requests - the server is truly stateless, with the
  //    exception of redis acting as a communication channel between the parent thread and
  //    worker threads. This means that multiple requests can't be handled at once without
  //    possibly overloading the parent machine
  // 2. The amount of data that can be processed at once is bottlenecked by
  //    how much can be inserted into redis at a time and how many individual
  //    processes the machine running the job can support
  // 3. Support for clusters of workers for the mapreduce engine is missing,
  //    meaning this solution would require additional development to work at scale.
export class Engine {
  constructor({ redisClient, logger, config }) {
    this.client = redisClient;
    this.logger = logger;
    this.config = config;
  }

  async run(jobId, payload) {
    const mapResults = [];
    while (payload.length) {
      const promises = [];

      // TODO: Add this limit to the config
      while (promises.length < 10 && payload.length) {
        const partialPayload = payload.splice(0, this.config.get('engine_chunk_size'));
        promises.push(
          this.operate('mapper')(jobId, partialPayload)
        )
      }

      const partialResults = await Bluebird.all(promises);

      // Remove the wrapping array from Bluebird.all
      mapResults.push(
        ..._.flatten(partialResults)
      );
    }

    const groupedResults = {};
    _.forEach(mapResults, result => {
      if (!groupedResults[result[0]]) {
        groupedResults[result[0]] = [];
      }

      groupedResults[result[0]].push(result[1]);
    })

    const finalGroupedResults = [];
    _.forEach(Object.keys(groupedResults), key => finalGroupedResults.push([key, groupedResults[key]]));

    const reduceResults = [];
    while (finalGroupedResults.length) {
      const promises = [];

      // TODO: Add this limit to the config
      while (promises.length < 10 && finalGroupedResults.length) {
        const partialPayload = finalGroupedResults.splice(0, this.config.get('engine_chunk_size'));
        promises.push(
          this.operate('reducer')(jobId, partialPayload)
        );
      }

      const partialResults = await Bluebird.all(promises);

      // Remove the wrapping array from Bluebird.all
      reduceResults.push(
        ..._.flatten(partialResults)
      );
    }

    return reduceResults;
  }

  operate(method) {
    return async (jobId, payload) => {
      const chunkId = Date.now();

      const redisPath = `mapreduce:${jobId}:${chunkId}`;

      // Write the resources to redis
      await this.client.setAsync(`${redisPath}:payload`, JSON.stringify(payload));

      const fork = cp.fork(`${__dirname}/${method}`, [jobId.toString(), chunkId.toString()]);

      await new Promise((resolve, reject) => {
        fork.on('message', message => {
          // TODO: Create a utility to manage formatting this message on both ends
          // to rule out human error when extending the code
          if (message === `${jobId}:${chunkId} COMPLETE`) {
            resolve();
          }
        });
      });

      const rawResults = await this.client.getAsync(`${redisPath}:results`);

      return JSON.parse(rawResults);
    }
  }
}
