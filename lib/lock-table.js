'use strict';
const dyno = require('./dyno');

const lockStatus = {
  TABLE_LOCKED: 1,
  TABLE_UNLOCKED: 2,
  TABLE_LOCK_EXISTS: 3,
  TABLE_ALREADY_UNLOCKED: 4,
  TABLE_LOCKED_BY_ANOTHER_CLIENT: 5,
  TABLE_UNLOCKED_BY_ANOTHER_CLIENT: 6,
  TABLE_ENTRY_NOT_EXISTS: 7,
  TABLE_ENTRY_CREATED: 8
};

class LockTable {
  constructor(lockTableName) {
    this._lockTableClient = dyno.getClient(lockTableName);
  }

  _get(lockedTableName) {
    return this._lockTableClient.getItem({
      Key: {
        tableName: lockedTableName
      }
    }).promise().then((data) => {
      const tableEntry = data.Item;
      return tableEntry;
    });
  }

  _update(lockedTableName, status) {
    return this._lockTableClient.updateItem({
      Key: {
        tableName: lockedTableName
      },
      UpdateExpression: 'SET #s = :s',
      ExpressionAttributeNames: {
        '#s' : 'status'
      },
      ConditionExpression: '(#s IN (:ns))',
      ExpressionAttributeValues: {
        ':s': status,
        ':ns': !status
      },
      ReturnValues: 'NONE'
    }).promise();
  }

  _put(lockedTableName, status) {
    return this._lockTableClient.putItem({
      Item: {
        tableName: lockedTableName,
        status: status
      },
      ConditionExpression: 'attribute_not_exists(#t)',
      ExpressionAttributeNames: { '#t': 'tableName' },
      ReturnValues: 'NONE'
    }).promise();
  }
}

function setLock(lockTable, lockedTableName) {
  return lockTable._get(lockedTableName)
    .then((tableEntry) => {
      if (tableEntry) {
        if (tableEntry.status === true) {
          return (lockStatus.TABLE_LOCK_EXISTS);
        } else {
          return lockTable._update(lockedTableName, true)
            .then(() => (lockStatus.TABLE_LOCKED))
            .catch((err) => {
              if (err.code === 'ConditionalCheckFailedException') {
                return (lockStatus.TABLE_LOCKED_BY_ANOTHER_CLIENT);
              }
              return err;
            });
        }
      } else {
        return lockTable._put(lockedTableName, true)
          .then(() => (lockStatus.TABLE_ENTRY_CREATED))
          .catch((err) => {
            if (err.code === 'ConditionalCheckFailedException') {
              return (lockStatus.TABLE_LOCKED_BY_ANOTHER_CLIENT);
            }
          });
      }
    });
}

function unsetLock(lockTable, lockedTableName) {
  return lockTable._get(lockedTableName)
    .then((tableEntry) => {
      if (tableEntry) {
        if (tableEntry.status === true) {
          return lockTable._update(lockedTableName, false)
            .then(() => (lockStatus.TABLE_UNLOCKED))
            .catch((err) => {
              if (err.code === 'ConditionalCheckFailedException') {
                return (lockStatus.TABLE_UNLOCKED_BY_ANOTHER_CLIENT);
              }
              return err;
            });
        } else {
          return (lockStatus.TABLE_ALREADY_UNLOCKED);
        }          
      } else {
        return (lockStatus.TABLE_ENTRY_NOT_EXISTS);
      }
    });
}

module.exports = LockTable;
module.exports.lockStatus = lockStatus;
module.exports.setLock = setLock;
module.exports.unsetLock = unsetLock;
