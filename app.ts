import { Result } from 'oxide.ts'
import { setTimeout } from 'timers/promises'

export type EventHandler<RecordType> = {
  getRecordId: (records: RecordType) => string
  onError?: (error: Error) => void
  onInfo?: (text: string) => void
}

type Record<RecordType> = { delay: number; tryCount: number; value: RecordType }

type TaskRunnerOptions<RecordType, ResultOkType, ResultErrorType> = {
  callback: (record: RecordType) => Promise<Result<ResultOkType, ResultErrorType>>
  eventHandler?: EventHandler<RecordType>
  records: RecordType[]
  retryAfterSeconds?: number
  retryDelayStepMilliseconds?: number
  concurrentTasks?: number
}

export class AsyncTaskRunner {
  public static async run<RecordType, ResultOkType, ResultErrorType>(
    options: TaskRunnerOptions<RecordType, ResultOkType, ResultErrorType>,
  ): Promise<void> {
    const retryAfterSeconds = options.retryAfterSeconds ?? 1
    const retryDelayStepMilliseconds = options.retryDelayStepMilliseconds ?? 100
    const concurrentTasks = options.concurrentTasks ?? 2

    const records: Record<RecordType>[] = options.records.map((value) => {
      return {
        delay: this.getDelay(retryAfterSeconds, concurrentTasks),
        tryCount: 0,
        value,
      }
    })

    const taskHandler = async () => {
      if (records.length <= 0) {
        return
      }

      const record = records.splice(0, 1)[0]
      await setTimeout(record.delay)
      options.eventHandler?.onInfo?.(`Processing record '${options.eventHandler.getRecordId(record.value)}'.`)
      const result = await options.callback(record.value)

      if (result.isErr()) {
        options.eventHandler?.onError?.(
          new Error(
            `Processing record '${String(options.eventHandler.getRecordId(record.value))}' resulted in error ${String(
              result.unwrapErr(),
            )} Wait for ${record.delay / 1000} seconds.`,
          ),
        )

        records.push({
          delay: retryAfterSeconds * 1000 + (record.tryCount + 1) * retryDelayStepMilliseconds,
          tryCount: record.tryCount + 1,
          value: record.value,
        })
      } else {
        options.eventHandler?.onInfo?.(
          `Record '${String(options.eventHandler.getRecordId(record.value))}' processed successfully.`,
        )
      }

      await taskHandler()
    }

    const taskHandlers = Array.from({ length: concurrentTasks }, taskHandler)

    // eslint-disable-next-line compat/compat
    await Promise.allSettled(taskHandlers)
  }

  private static getDelay(retryAfterMilliseconds: number, simultaneousTasks: number): number {
    return retryAfterMilliseconds * 1000 + Math.floor(Math.random() * simultaneousTasks) * 200
  }
}