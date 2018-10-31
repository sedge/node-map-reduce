import _ from 'lodash';

export default [
  {
    id: 1,

    name: 'transactionCount',
    method: 'post',
    endpoint: '/count-merchant-transactions',
    schema: { }, // TODO: Add schema validation

    mapper: transaction => [transaction.merchant, 1],
    reducer: transactionsByMerchant => [ transactionsByMerchant[0], _.sum(transactionsByMerchant[1]) ],

    toOutput: results => ({
      merchant_transaction_count: results
    })
  },
  {
    id: 2,

    name: 'happinessPercentagePerUser',
    method: 'post',
    endpoint: '/calculate-user-happiness',
    schema: { }, // TODO: Add schema validation

    mapper: transaction => {
      if (transaction.user_id) {
        return [transaction.user_id, { reflected: transaction.reflected }];
      }

      return [transaction.id, { name: transaction.name }];
    },
    reducer: transactionsByUser => {
      const data = transactionsByUser[1];

      const name = _.find(data, datum => datum.name).name;
      const reflectionData = _.filter(data, datum => datum.reflected);
      const goodReflectionData = _.filter(
        reflectionData,
        reflectionDatum => reflectionDatum.reflected === 'GOOD'
      );

      return [
        name,
        `${
          Math.round(
            goodReflectionData.length / reflectionData.length * 100
          )
        }%`
      ];
    },

    toOutput: results => ({
      user_happiness: results
    })
  },
];
