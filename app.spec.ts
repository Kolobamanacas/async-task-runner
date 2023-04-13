import { Err, Ok, Result } from 'oxide.ts'
import { setTimeout } from 'timers/promises'

import { AsyncTaskRunner } from '@/server/shared/libs/async-task-runner/async-task-runner'

describe('AsyncTaskRunner > run', () => {
  const callbackOkMock = jest.fn(async (_record: string): Promise<Result<true, Error>> => {
    await setTimeout(50)

    return Ok(true)
  })

  beforeEach(() => {
    callbackOkMock.mockClear()
  })

  it.each([
    { records: [] },
    { records: ['A'] },
    { records: ['A', 'B', 'C'] },
    { records: ['A', 'B', 'C', 'D', 'E', 'F', 'G'] },
  ])(
    'Calls callback $records.length times when $records.length records are passed and every callback returns Ok',
    async ({ records }) => {
      await AsyncTaskRunner.run<string, true, Error>({
        callback: callbackOkMock,
        concurrentTasks: 3,
        records,
        retryAfterSeconds: 1,
        retryDelayStepMilliseconds: 100,
      })

      expect(callbackOkMock).toBeCalledTimes(records.length)
    },
  )

  it.each([
    { errorsBeforeOk: 0, expectedCalls: 0, records: [] },
    { errorsBeforeOk: 0, expectedCalls: 1, records: ['A'] },
    { errorsBeforeOk: 0, expectedCalls: 2, records: ['A', 'B'] },
    { errorsBeforeOk: 0, expectedCalls: 3, records: ['A', 'B', 'C'] },
    { errorsBeforeOk: 1, expectedCalls: 2, records: ['A'] },
    { errorsBeforeOk: 1, expectedCalls: 3, records: ['A', 'B'] },
    { errorsBeforeOk: 2, expectedCalls: 3, records: ['A'] },
    { errorsBeforeOk: 2, expectedCalls: 4, records: ['A', 'B'] },
    { errorsBeforeOk: 3, expectedCalls: 5, records: ['A', 'B'] },
  ])(
    'Calls callback $expectedCalls times when $records.length records are passed and every callback returns $errorsBeforeOk errors before Ok',
    async ({ errorsBeforeOk, expectedCalls, records }) => {
      const getMockWithErrors = <RecordType>() => {
        let errorsCount = errorsBeforeOk + 1

        return jest.fn(async (_record: RecordType): Promise<Result<true, Error>> => {
          await setTimeout(50)
          errorsCount -= 1

          if (errorsCount > 0) {
            return Err(new Error())
          }

          return Ok(true)
        })
      }

      const callbackMock = getMockWithErrors<string>()

      await AsyncTaskRunner.run<string, true, Error>({
        callback: callbackMock,
        concurrentTasks: 3,
        records,
        retryAfterSeconds: 1,
        retryDelayStepMilliseconds: 1,
      })

      expect(callbackMock).toBeCalledTimes(expectedCalls)
    },
  )
})
