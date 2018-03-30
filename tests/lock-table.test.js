'use strict';
const tape = require('tape');
const lockTable = require('../lib/lock-table');
const sinon = require('sinon');

tape('SetLock to a locked Table', (assert) => {
  const tableEntry = {
    tableName: 'employeeDetails',
    status: true
  }

  const table = {
    _get: sinon.stub().returns(Promise.resolve(tableEntry))
  }

  lockTable.setLock(table, 'employeeDetails')
    .then((data) => {
      assert.deepEquals(data, lockTable.lockStatus.TABLE_LOCK_EXISTS);
      assert.end();
    });
});

tape('SetLock to an unlocked Table', (assert) => {
  const tableEntry = {
    tableName: 'employeeDetails',
    status: false
  }

  const updatedTableEntry = {
    tableName: 'employeeDetails',
    status: true    
  }

  const table = {
    _get: sinon.stub().returns(Promise.resolve(tableEntry)),
    _update: sinon.stub().returns(Promise.resolve(updatedTableEntry))
  }

  lockTable.setLock(table, 'employeeDetails')
    .then((data) => {
      assert.deepEquals(data, lockTable.lockStatus.TABLE_LOCKED);
      assert.end();
    });
});

tape('SetLock to a Table first time', (assert) => {
  const putTableEntry = {
    tableName: 'employeeDetails',
    status: true    
  }

  const table = {
    _get: sinon.stub().returns(Promise.resolve(undefined)),
    _put: sinon.stub().returns(Promise.resolve(putTableEntry))
  }

  lockTable.setLock(table, 'employeeDetails')
    .then((data) => {
      assert.deepEquals(data, lockTable.lockStatus.TABLE_ENTRY_CREATED);
      assert.end();
    });
});

tape('SetLock to an unlocked Table but with another client locking table in between', (assert) => {
  const tableEntry = {
    tableName: 'employeeDetails',
    status: false
  }

  const table = {
    _get: sinon.stub().returns(Promise.resolve(tableEntry)),
    _update: sinon.stub().returns(Promise.reject({code: 'ConditionalCheckFailedException'}))
  }

  lockTable.setLock(table, 'employeeDetails')
    .then((data) => {
      assert.deepEquals(data, lockTable.lockStatus.TABLE_LOCKED_BY_ANOTHER_CLIENT);
      assert.end();
    });
});

tape('UnsetLock to a locked Table', (assert) => {
  const tableEntry = {
    tableName: 'employeeDetails',
    status: true
  }

  const updatedTableEntry = {
    tableName: 'employeeDetails',
    status: false    
  }

  const table = {
    _get: sinon.stub().returns(Promise.resolve(tableEntry)),
    _update: sinon.stub().returns(Promise.resolve(updatedTableEntry))
  }

  lockTable.unsetLock(table, 'employeeDetails')
    .then((data) => {
      assert.deepEquals(data, lockTable.lockStatus.TABLE_UNLOCKED);
      assert.end();
    });
});

tape('UnsetLock to an unlocked Table', (assert) => {
  const tableEntry = {
    tableName: 'employeeDetails',
    status: false
  }

  const table = {
    _get: sinon.stub().returns(Promise.resolve(tableEntry))
  }

  lockTable.unsetLock(table, 'employeeDetails')
    .then((data) => {
      assert.deepEquals(data, lockTable.lockStatus.TABLE_ALREADY_UNLOCKED);
      assert.end();
    });
});

tape('UnsetLock to an inexistent Table', (assert) => {
  const table = {
    _get: sinon.stub().returns(Promise.resolve(undefined))
  }

  lockTable.unsetLock(table, 'employeeDetails')
    .then((data) => {
      assert.deepEquals(data, lockTable.lockStatus.TABLE_ENTRY_NOT_EXISTS);
      assert.end();
    });
});

tape('UnsetLock to a locked Table but with another client unlocking table in between', (assert) => {
  const tableEntry = {
    tableName: 'employeeDetails',
    status: true
  }

  const table = {
    _get: sinon.stub().returns(Promise.resolve(tableEntry)),
    _update: sinon.stub().returns(Promise.reject({code: 'ConditionalCheckFailedException'}))
  }

  lockTable.unsetLock(table, 'employeeDetails')
    .then((data) => {
      assert.deepEquals(data, lockTable.lockStatus.TABLE_UNLOCKED_BY_ANOTHER_CLIENT);
      assert.end();
    });
});
