import express from 'express';

import { Engine } from './engine';

export function buildHandler({ job, redisClient, redlock, logger, config }) {
  const router = express.Router()

  const {
    id,
    method,
    endpoint,
    toOutput
  } = job;

  router[method](endpoint, async (req, res, next) => {
    const payload = req.body;
    const engine = new Engine({ redisClient, redlock, logger, config });

    const result = await engine.run(id, payload);

    res.json(
      toOutput(result)
    );
  });

  return router;
}
