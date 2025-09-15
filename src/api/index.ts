import server from './server'

// Add global handler for uncaught Kafka errors
process.on('uncaughtException', (error) => {
  if (error.message?.includes('broker transport failure') || error.message?.includes('kafka')) {
    console.log('=== KAFKA UNCAUGHT ERROR ===')
    console.log('Error message:', error.message)
    console.log('Error stack:', error.stack)
    console.log('Continuing without crashing...')
    console.log('============================')
    return // Don't crash the process for Kafka errors
  }
  // Re-throw non-Kafka errors
  throw error
})

export default server.run();