import * as fs from 'fs'
import * as path from 'path'
import { KV } from './type'

type MapFunction = (filename: string, contents: string) => KV[]
type ReduceFunction = (key: string, values: string[]) => string

export default class MapReduceFramework {
  private config: {
    jobName: string
    inputFiles: string[]
    reduceCount: number
    workDir: string
  }
  private mapFunc: MapFunction
  private reduceFunc: ReduceFunction

  constructor(
    jobName: string,
    inputFiles: string[],
    reduceCount: number,
    mapFunc: MapFunction,
    reduceFunc: ReduceFunction,
    workDir: string = './work'
  ) {
    this.config = { 
      jobName, 
      inputFiles, 
      reduceCount,
      workDir 
    }
    this.mapFunc = mapFunc
    this.reduceFunc = reduceFunc

    // Ensure work directory exists
    this.initializeWorkDirectory()
  }

  private initializeWorkDirectory(): void {
    if (!fs.existsSync(this.config.workDir)) {
      fs.mkdirSync(this.config.workDir, { recursive: true })
    }
  }

  execute(): void {
    this.mapPhase()
    this.reducePhase()
  }

  private mapPhase(): void {
    this.config.inputFiles.forEach((file, mapTaskId) => {
      try {
        const contents = this.readFile(file)
        const keyValues = this.mapFunc(file, contents)
        this.partitionMapOutput(keyValues, mapTaskId)
      } catch (error) {
        console.error(`Error processing map task for ${file}:`, error)
      }
    })
  }

  private partitionMapOutput(keyValues: KV[], mapTaskId: number): void {
    // Group key-value pairs by reduce task
    const partitions: Record<number, KV[]> = {}

    keyValues.forEach(kv => {
      // Simple hash-based partitioning
      const reduceTaskId = this.getPartitionForKey(kv.key)
      
      if (!partitions[reduceTaskId]) {
        partitions[reduceTaskId] = []
      }
      partitions[reduceTaskId].push(kv)
    })

    // Write partitioned results to intermediate files
    Object.entries(partitions).forEach(([reduceTaskId, partition]) => {
      const filename = this.getIntermediateFileName(mapTaskId, parseInt(reduceTaskId))
      this.writeIntermediateFile(filename, partition)
    })
  }

  private reducePhase(): void {
    const results: { key: string, value: string }[] = [];

    // Collect and group intermediate files for each reduce task
    for (let reduceTaskId = 0; reduceTaskId < this.config.reduceCount; reduceTaskId++) {
      const intermediateFiles = this.findIntermediateFiles(reduceTaskId)
      const groupedValues = this.groupIntermediateValues(intermediateFiles)

      // Apply reduce function to each key group
      Object.entries(groupedValues).forEach(([key, values]) => {
        const reducedValue = this.reduceFunc(key, values)
        results.push({ key, value: reducedValue })
      })
    }

    // Write final results
    this.writeFinalResults(results)
  }

  private getPartitionForKey(key: string): number {
    // Simple hash-based partitioning
    return Math.abs(this.hashCode(key)) % this.config.reduceCount
  }

  private hashCode(str: string): number {
    let hash = 0
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i)
      hash = ((hash << 5) - hash) + char
      hash = hash & hash // Convert to 32-bit integer
    }
    return hash
  }

  private getIntermediateFileName(mapTaskId: number, reduceTaskId: number): string {
    return path.join(
      this.config.workDir, 
      `${this.config.jobName}-map-${mapTaskId}-reduce-${reduceTaskId}.json`
    )
  }

  private readFile(filename: string): string {
    try {
      return fs.readFileSync(filename, 'utf-8')
    } catch (error) {
      console.error(`Error reading file ${filename}:`, error)
      return ''
    }
  }

  private writeIntermediateFile(filename: string, keyValues: KV[]): void {
    fs.writeFileSync(filename, JSON.stringify(keyValues, null, 2))
  }

  private findIntermediateFiles(reduceTaskId: number): string[] {
    return fs.readdirSync(this.config.workDir)
      .filter(file => 
        file.includes(`-reduce-${reduceTaskId}.json`) && 
        file.startsWith(this.config.jobName)
      )
      .map(file => path.join(this.config.workDir, file))
  }

  private groupIntermediateValues(files: string[]): Record<string, string[]> {
    const groupedValues: Record<string, string[]> = {}

    files.forEach(file => {
      const fileContent = JSON.parse(fs.readFileSync(file, 'utf-8'))
      fileContent.forEach((kv: KV) => {
        if (!groupedValues[kv.key]) {
          groupedValues[kv.key] = []
        }
        groupedValues[kv.key].push(kv.value)
      })
    })

    return groupedValues
  }

  private writeFinalResults(results: { key: string, value: string }[]): void {
    const outputFile = path.join(
      this.config.workDir, 
      `${this.config.jobName}-output.json`
    )
    fs.writeFileSync(outputFile, JSON.stringify(results, null, 2))
    console.log(`Results written to ${outputFile}`)
  }
}
