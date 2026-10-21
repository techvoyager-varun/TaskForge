export const testDatabase = process.env.TEST_DATABASE_URL;
if (!testDatabase || !/(?:_test|test_)/.test(new URL(testDatabase).pathname))
  throw new Error('TEST_DATABASE_URL must explicitly name an isolated test database');
