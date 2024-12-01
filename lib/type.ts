type jobStatus = 'idle' | 'mapping' | 'reducing'

export interface KV<T = string> {
    key: string,
    value: T
}

export enum JobPhase {
    MAP,
    REDUCE
}

export interface Job {
    name: string,
    files: string[],
    mapFunction: (file: string, contents: string) => KV[],
    reduceFunction: (key: string, values: string[]) => string,
    reduceNum: number
}

export interface Worker {
    id: string,
    address: string,
    status: jobStatus
}

export interface Task {
    id: string,
    name: string,
    phase: JobPhase,
    file?: string,
    reduceKey?: string
}
