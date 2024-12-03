import { writeFileSync } from "fs"

type KV = {key: string, value: number}
type Reduce = { [word: string]: number; }

interface WordCountChallenge {
  map(text: string): KV[];
  reduce(mapped: KV[]): Reduce;
}

class WordCount implements WordCountChallenge {
    map(text: string): KV[] {
        const map: KV[] = []
        for (const word of text.split(" ")) {
            map.push({ key: word, value: 1 })
        }
        return map
    }
    reduce(mapped: KV[]): Reduce {
        const reduce: Reduce = {}
        for (const m of mapped) {
                if (reduce[m.key] === undefined) reduce[m.key] = m.value
                else reduce[m.key] += m.value
        }
        return reduce
    }
}

const input = "lorem ipsum lorem dolor ipsum lorem dolor amet sir amet sir selamat ulang tahun dan bahagia selalu"
const wordCount = new WordCount()
const result = wordCount.reduce(wordCount.map(input))
writeFileSync("output.json", JSON.stringify(result))
