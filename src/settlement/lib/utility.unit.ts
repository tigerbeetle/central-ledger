import { describe, it } from "node:test";
import { _generalTopicTemplate } from "./utility";
import assert from "node:assert";

describe('utility', () => {
  describe('generalTopicTemplate', () => {
    it('generates the topic name given valid inputs', () => {
      // Arrange
      // Act
      const result = _generalTopicTemplate('thing1', 'thing2')

      // Assert
      assert.strictEqual(result, 'topic-thing1-thing2')
    })

    // This throw an error, but maintaining existing functionality
    it('allows invalid input', () => {
      const result = _generalTopicTemplate(undefined, 'thing2')
      assert.strictEqual(result, 'topic--thing2')
    })
  })
})