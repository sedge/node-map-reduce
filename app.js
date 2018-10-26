import express from 'express';

// import jobs from 'jobs';

export function constructServer ({ redisClient, logger, config }) {
	return express();
}

// Todo:
	// Build `job` module abstraction
		// HTTP method + endpoint
		// Map function
		// Reduce function
		// Payload validation with joi
		// Output mapping function
	// Build generic request handler and dynamic route configuration
	// Build Map/Reduce engine
		// Build redis queue schema
		// Build cluster logic to co-ordinate threads
		// Code reduction handler
			// Enforcing locking
			// Code automatic retry

	// Code integration test coverage

