import Master from "./lib/master"
import Worker from "./lib/slave"
import { JobPhase, KV } from "./lib/type"
import MapReduceFramework from "./lib/framework"

function simple() {
    function wordCountMap(_: string, contents: string): KV[] {
        return contents
        .toLowerCase()
        .match(/\b\w+\b/g)
            ?.map(word => ({ key: word, value: '1' })) || []
    }

    function wordCountReduce(_: string, values: string[]): string {
        return values.reduce((sum, val) => sum + parseInt(val), 0).toString()
    }

    const job = new MapReduceFramework(
        'word-count',
        ['sample1.txt', 'sample2.txt'],
        3,
        wordCountMap,
        wordCountReduce
    )

    job.execute()
}

function complex() {
    const master = new Master<number>()

    for (let i = 0; i < 5; i++) {
        const worker = new Worker('http://localhost:3000/master')
            worker.on('registered', console.log)
        worker.on('registrationError', console.error)
        worker.on('taskFailed', console.error)
        worker.on('writeIntermediateResultError', console.error)
        worker.on('writeFinalResultError', console.error)
        worker.on('shutdown', console.log)
        worker.on('shutdownError', console.error)
        worker.on('startError', console.error)
        worker.start()
        master.registerWorker(worker.masterAddress)
    }

    const mapF = Worker.performMap
    const reduceF = Worker.performReduce

    master.initJob({
        name: 'wordCount',
        files: ['sample1.txt', 'sample2.txt'],
        mapFunction: mapF,
        reduceFunction: reduceF,
        reduceNum: 3
    })

    while (!master.isCompleted(JobPhase.REDUCE)) {
        const task = master.assignTask()

        if (task) {
            const workers = master.getWorkers()
            workers.forEach(worker => {
                worker.executeTask(task)
                master.taskCompleted(task.id)
            })
        }
    }

    console.log('Job completed')
}

process.argv[2] === 'simple' ? simple() : complex()
