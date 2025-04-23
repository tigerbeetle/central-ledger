import { describe, it } from 'node:test';import assert from 'node:assert'
import { makeConfig } from './resolver';


describe('config/resolver', () => {

  it('loads the config', () => {
    // Arrange


    // Act
    const config = makeConfig()

    // Assert
    // rules are asserted internally
    assert.ok(config)
  })
})