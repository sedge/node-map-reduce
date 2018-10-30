import joi from 'joi';


export default [
  {
    id: 1,

    name: 'transactionCount',
    method: 'post',
    endpoint: '/count-merchant-transactions',
    schema: { }, // TODO: Add schema validation

    mapper: transactions => [transaction.merchant, 1],
    reducer: transactionsByMerchant => [ transactionsByMerchant[0], _.sum(transactionsByMerchant[1]) ],

    toOutput: results => ({
      merchant_transaction_count: results
    })
  },
];
