import EventEmitter from "events";
import { randomUUID } from "crypto";
import path from "path";
import fs from "fs";
import { JobPhase, KV, Task, jobStatus } from "./type";

/**
* Worker class for map-reduce job
* @extends EventEmitter
* @example
const worker = new Worker('http://localhost:3000/master')

worker.on('registered', console.log)
worker.on('registrationError', console.error)
worker.on('taskFailed', console.error)
worker.on('writeIntermediateResultError', console.error)
worker.on('writeFinalResultError', console.error)
worker.on('shutdown', console.log)
worker.on('shutdownError', console.error)

worker.start()

const task: Task = {
    id: 'map-task-1',
    name: 'wordCount',
    phase: JobPhase.MAP,
    file: path.join(__dirname, 'input.txt')
}

const result = worker.executeTask(task)
*/
export default class Worker extends EventEmitter {
    public id: string
    public status: jobStatus
    public masterAddress: string
    private workDir: string

    constructor(masterAddress: string, workDir?: string) {
        super()
        this.id = randomUUID()
        this.masterAddress = masterAddress
        this.workDir = workDir || path.join(process.cwd(), 'work')
    }

    /** Start the worker with creating the work directory */
    start() {
        fs.mkdir(
            this.workDir,
            async err => { err ? this.emit('startError', err) : await this.registerWithMaster() }
        )
    }

    /** Register the worker with the master */
    private async registerWithMaster() {
        try {
            const res = await fetch(`${this.masterAddress}/workerRegister`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id: this.id, address: this.getWorkerAddress() })
            })
            this.emit('registered', await res.json())
        } catch (err) {
            this.emit('registrationError', err)
            throw err
        }
    }

    /**
    * Execute worker task either map or reduce tasks
    * @param Task task to do and it's phase (map/reduce)
    */
    executeTask(task: Task) {
        try {
            switch (task.phase) {
                case JobPhase.MAP:
                    return this.executeMap(task)
                case JobPhase.REDUCE:
                    return this.executeReduce(task)
                default:
                    throw new Error('Unspported job phase: ' + task.phase)
            }
        } catch (err) {
            this.emit('taskFailed', { task, err })
            throw err
        }
    }

    /**
    * Execute map task
    * @param Task the map task
    * @return Object of task map and it's intermediateResultCount
    */
    private executeMap(task: Task) {
        if (!task.file) throw new Error('No input file specified for map task')

        this.status = 'mapping'
        fs.readFile(task.file, async (err, file) => {
            if (err) throw err
            const intermediateResult = Worker.performMap(task.file, file.toString())
            await this.writeIntermediateResult(task.name, task.id, intermediateResult)

            return { taskId: task.id, intermediateResultCount: intermediateResult.length }
        })
    }

    /**
    * Execute reduce task
    * @param Task the reduce task
    * @return Object of task id and result key
    */
    private async executeReduce(task: Task) {
        if (!task.reduceKey) throw new Error('No reduce key specified for reduce task')

        this.status = 'reducing'
        const intermediateValues = await this.collectIntermediateValues(task.name, task.reduceKey)

        const result = Worker.performReduce(task.reduceKey, intermediateValues)

        this.writeFinalResult(task.name, task.id, result)

        return { taskId: task.id, resultKey: task.reduceKey }
    }

    /**
    * Utility method for intermediate data handling
    * @param name the task name
    * @param id the task id
    * @param KV[] the intermediate results
    * @return Promise of writing intermediate results
    */
    private async writeIntermediateResult<T>(name: string, id: string, results: KV<T>[]) {
        const outputDir = path.join(this.workDir, name, id)

        fs.mkdir(outputDir, err => { err ? this.emit('writeIntermediateResultError', err) : null })

        const promises = results.map(async (kv, i) => {
            const filename = path.join(outputDir, `${i}-${kv.key}.json`)
            fs.writeFile(filename, JSON.stringify(kv), err => 
             { err ? this.emit('writeIntermediateResultError', err) : null }
            )
        })
        
        await Promise.all(promises)
    }

    /**
    * Utility method for final data handling
    * @param name the task name
    * @param reduceKey the reduce key
    * @return Promise of writing final results
    */
    private async collectIntermediateValues(name: string, reduceKey: string): Promise<string[]> {
        const intermediateDir = path.join(this.workDir, name)
        let values: Promise<string>[] = []

        fs.readdir(intermediateDir, (err, files) => {
            if (err) throw err

            const matchingFiles = files.filter(f => f.includes(reduceKey))
            const valuePromises = matchingFiles.map(file => {
                let value: Promise<string>
                fs.readFile(path.join(intermediateDir, file), 'utf8', (err, data) => {
                    if (err) throw err
                    const kv = JSON.parse(data)
                    value = kv.value
                })
                return value!
            })

            values = valuePromises
        })

        return Promise.all(values)
    }

    /**
    * Utility static method for map function example (word count)
    * @param filename the input file name
    * @param contents the file contents
    * @return KV[] the key-value pairs
    */
    static performMap(_: string | undefined, contents: string): KV<number>[] {
        return contents.toLowerCase().split(/\s+/).map(word => ({ key: word, value: 1 }))
    }

    /**
    * Utility static method for reduce function example (word count)
    * @param key the key to reduce
    * @param values the values to reduce
    * @return string the reduced value
    */
    static performReduce(_: string, values: string[]): string {
        return values.length.toString()
    }

    /**
    * Write final result to file
    * @param name the task name
    * @param id the task id
    * @param result the final result
    */
    private writeFinalResult(name: string, id: string, result: string) {
        const outputDir = path.join(this.workDir, name, id, 'result.json')
        fs.writeFile(outputDir, JSON.stringify(result), err => {
            if (err) this.emit('writeFinalResultError', err)
        })
        this.status = 'idle'
    }

    /**
    * Get worker address
    * @return string the worker address
    */
    private getWorkerAddress(): string {
        // In real world, this should be the worker's IP address
        return `http://localhost:${Math.floor(Math.random() * 1000) + 3000}`
    }

    async shutdown() {
        try {
            await fetch(`${this.masterAddress}/workerShutdown`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id: this.id })
            })

            fs.rmdir(this.workDir, { recursive: true }, err => { if (err) throw err })

            this.emit('shutdown')
        } catch (err) {
            this.emit('shutdownError', err)
            throw err
        }
    }
}
