import { writeFileSync } from "fs"

// module 1: Grade distribution
type KV = {key: string, value: number}
type Reduce = { [grade: string]: number }
const studentScores = [85, 92, 78, 65, 90, 55, 88, 72]
/** const ouput = {
  'A': 3,
  'B': 1,
  'C': 2,
  'D': 1,
  'F': 1 
} */

interface MapReduceChallange {
    map(scores: number[]): KV[]
    reduce(mapped: KV[]): {[grade: string]: number}
}

class MapReduce implements MapReduceChallange {
    reduce(map: KV[]): Reduce {
        const reduce: Reduce = {}
        for (const m of map) {
            if (reduce[m.key] === undefined) reduce[m.key] = 1
            else reduce[m.key] += 1
        }
        return reduce
    }
    map(scores: number[]): KV[] {
        const map: KV[] = []
        for (const s of scores) {
            map.push({ key: this.getGrade(s), value: s })
        }
        return map
    }

    private getGrade(s: number): string {
        if (s >= 90) return 'A'
        if (s >= 80) return 'B'
        if (s >= 70) return 'C'
        if (s >= 60) return 'D'
        return 'F'
    }
}

const mapReduce = new MapReduce()
const result = mapReduce.reduce(mapReduce.map(studentScores))
writeFileSync("output.json", JSON.stringify(result))
