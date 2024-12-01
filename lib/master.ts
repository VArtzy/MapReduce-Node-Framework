import EventEmitter from "events"
import { Job, Task, JobPhase } from "./type"
import Worker from "./slave"
import { randomUUID } from "crypto"

/**
* Master class for managing workers and tasks
* @extends EventEmitter
* @example
const mapF = (file: string, contents: string) => { return [{ key: 'key1', value: 'value1' }] }
const reduceF = (key: string, values: string[]) => { return 'value1' }
const master = new Master()

const worker1 = master.registerWorker('http://worker1.local')
const worker2 = master.registerWorker('http://worker2.local')

master.initJob({
    name: 'wordCount',
    files: ['sample1.txt', 'sample2.txt'],
    mapFunction: mapF,
    reduceFunction: reduceF,
    reduceNum: 3
})

const nextTask = master.assignTask()
*/
export default class Master<T> extends EventEmitter {
    private workers: Map<string, Worker> = new Map()
    private tasks: Task[] = []
    private completedTasks: Task[] = []
    private job: Job<T> | null = null

    constructor() { super() }

    /**
    * Register a worker to the master
    * @param address The address of the worker
    * @return The id of the worker
    */
    registerWorker(address: string): string {
        const workerId = randomUUID()
        const worker = new Worker(address)
        this.workers.set(workerId, worker)
        this.emit('workerRegister', worker)
        return workerId
    }

    /**
    * Assign a task to a worker. creating multiple, dynamic runtime job
    * @param Job The job to be executed
    */
    initJob(job: Job<T>) {
        if (this.job) {
            throw new Error('Job already exists')
        }

        this.job = job
        this.completedTasks = []

        this.map()
    }

    /** create a task for each file */
    private map() {
        if (!this.job) return

        this.tasks = this.job.files.map((file, id) => ({
            id: id.toString(),
            name: this.job!.name,
            phase: JobPhase.MAP,
            file
        }))
    }

    /**
    * Handle the task completion
    * @return The next task to be assigned
    */
    assignTask(): Task | null {
        const idleWorker = Array.from(this.workers.values()).find(worker => worker.status === 'idle')
        if (!idleWorker) return null

        const unassignedTask = this.tasks.find(task => !this.completedTasks.some(ct => ct.id === task.id))
        return unassignedTask || null
    }

    /**
    * Mark the task as completed and reduce if all tasks are completed
    * @param taskId The id of the task
    */
    taskCompleted(taskId: string) {
        const task = this.tasks.find(t => t.id === taskId)
        if (task) {
            this.completedTasks.push(task)
            this.emit('taskCompleted', task)

            if (this.isCompleted(JobPhase.MAP)) {
                this.reduce()
            }
        }
    }

    /** Collect the intermediate keys from all map tasks */
    private reduce() {
        if (!this.job) return

        const intermediate = this.collectIntermediate()

        this.tasks = intermediate.map((key, idx) => ({
            id: idx.toString(),
            name: this.job!.name,
            phase: JobPhase.REDUCE,
            reduceKey: key
        }))

        this.completedTasks = []
    }

    private collectIntermediate(): string[] {
        // TODO: Implement this
        return ['key1', 'key2', 'key3']
    }

    /**
    * Check if all tasks are completed for a given phase
    * @param jobPhase The phase of the job
    * @return boolean indicating if all tasks are completed
    */
    isCompleted(jobPhase: JobPhase): boolean {
        return this.tasks.length > 0 && this.tasks.length === this.completedTasks.length
        && this.completedTasks.every(task => task.phase === jobPhase)
    }

    /**
    * Utility method for geting workers
    * @return Array of workers
    */
    getWorkers(): Worker[] {
        return Array.from(this.workers.values())
    }

    /**
    * Uitlity method for geting current job
    * @returns Current job
    */
    getCurrentJob(): Job<T> | null {
        return this.job
    }
}
