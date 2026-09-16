/**
 * The folder tree derived from the flat list the runner sends.
 *
 * What these pin is mostly about a library nobody wrote for the Studio: paths
 * several levels deep, folders that hold only other folders, numbered file names,
 * and the fact that browsing must never be confused with owning — nothing here
 * produces a record, only a reading of what is on disk.
 */

import { describe, expect, it } from 'vitest'

import type { LibraryFile } from '@/lib/runner/libraryFiles'
import {
  crumbsOf,
  folderOf,
  isUnder,
  levelOf,
  normalizeFolder,
  parentOf,
  searchFiles,
} from '@/lib/library/tree'

function file(path: string): LibraryFile {
  const name = path.split('/').pop()?.replace(/\.json$/, '') ?? path
  return { path, name, size: 100, modified: 1_756_400_000 }
}

const LIBRARY = [
  file('vendas/bronze/ingestao.json'),
  file('vendas/gold/resumo.json'),
  file('vendas/silver/limpeza.json'),
  file('compras/ingestao.json'),
  file('avulso.json'),
]

describe('normalizeFolder', () => {
  it('treats every spelling of the root as the root', () => {
    for (const spelling of ['', '/', '//', '.', './']) {
      expect(normalizeFolder(spelling)).toBe('')
    }
  })

  it('drops leading, trailing and doubled slashes', () => {
    expect(normalizeFolder('/vendas//bronze/')).toBe('vendas/bronze')
  })
})

describe('parentOf', () => {
  it('walks one level up', () => {
    expect(parentOf('vendas/bronze/ingestao.json')).toBe('vendas/bronze')
  })

  it('stops at the root instead of going past it', () => {
    expect(parentOf('avulso.json')).toBe('')
    expect(parentOf('')).toBe('')
  })

  it('is where a file lives', () => {
    expect(folderOf(file('vendas/gold/resumo.json'))).toBe('vendas/gold')
  })
})

describe('crumbsOf', () => {
  it('always starts at the root', () => {
    expect(crumbsOf('')).toEqual([{ path: '', name: 'Library' }])
  })

  it('names every step with the segment, not with the whole path', () => {
    expect(crumbsOf('vendas/bronze')).toEqual([
      { path: '', name: 'Library' },
      { path: 'vendas', name: 'vendas' },
      { path: 'vendas/bronze', name: 'bronze' },
    ])
  })

  it('lets the caller name the root after the directory it opened', () => {
    expect(crumbsOf('', 'sparquet-confs')[0].name).toBe('sparquet-confs')
  })
})

describe('isUnder', () => {
  it('counts the folder itself', () => {
    expect(isUnder('vendas', 'vendas')).toBe(true)
  })

  it('does not count a sibling whose name merely starts the same', () => {
    // `vendas-2023` must not show up inside `vendas`.
    expect(isUnder('vendas-2023/a.json', 'vendas')).toBe(false)
  })

  it('puts everything under the root', () => {
    expect(isUnder('vendas/bronze/a.json', '')).toBe(true)
  })
})

describe('levelOf', () => {
  it('shows the folders of the root and the files loose in it', () => {
    const level = levelOf(LIBRARY)

    expect(level.folders.map((folder) => folder.name)).toEqual(['compras', 'vendas'])
    expect(level.files.map((item) => item.path)).toEqual(['avulso.json'])
  })

  it('counts everything underneath a folder, not only its own files', () => {
    // A library organised as domain/layer/job.json is all "empty" folders at the
    // top; a count of 0 there would read as nothing to open.
    const [compras, vendas] = levelOf(LIBRARY).folders

    expect(compras.fileCount).toBe(1)
    expect(vendas.fileCount).toBe(3)
  })

  it('descends into a folder', () => {
    const level = levelOf(LIBRARY, 'vendas')

    expect(level.folders.map((folder) => folder.path)).toEqual([
      'vendas/bronze',
      'vendas/gold',
      'vendas/silver',
    ])
    expect(level.files).toEqual([])
  })

  it('reaches the files at the bottom', () => {
    expect(levelOf(LIBRARY, 'vendas/bronze').files.map((item) => item.path)).toEqual([
      'vendas/bronze/ingestao.json',
    ])
  })

  it('accepts a folder path spelled with stray slashes', () => {
    expect(levelOf(LIBRARY, '/vendas/').folders).toHaveLength(3)
  })

  it('is empty for a folder that is not there rather than throwing', () => {
    expect(levelOf(LIBRARY, 'nao-existe')).toEqual({ folders: [], files: [] })
  })

  it('sorts numbered names the way a person reads them', () => {
    const numbered = [file('etl/stage10.json'), file('etl/stage2.json')]

    expect(levelOf(numbered, 'etl').files.map((item) => item.name)).toEqual([
      'stage2',
      'stage10',
    ])
  })

  it('does not let a sibling prefix leak into a folder', () => {
    const tricky = [file('vendas/a.json'), file('vendas-2023/b.json')]

    expect(levelOf(tricky, 'vendas').files.map((item) => item.path)).toEqual(['vendas/a.json'])
  })

  it('holds an empty library without failing', () => {
    expect(levelOf([])).toEqual({ folders: [], files: [] })
  })
})

describe('searchFiles', () => {
  it('finds a file from two pieces of its path, in any order', () => {
    expect(searchFiles(LIBRARY, 'gold vendas').map((item) => item.path)).toEqual([
      'vendas/gold/resumo.json',
    ])
  })

  it('ignores case', () => {
    expect(searchFiles(LIBRARY, 'INGESTAO')).toHaveLength(2)
  })

  it('searches the whole library, not the folder being browsed', () => {
    expect(searchFiles(LIBRARY, 'ingestao').map((item) => item.path)).toEqual([
      'compras/ingestao.json',
      'vendas/bronze/ingestao.json',
    ])
  })

  it('answers nothing for an empty query instead of everything', () => {
    // An empty search box means "I am browsing", not "show me all two hundred".
    expect(searchFiles(LIBRARY, '   ')).toEqual([])
  })
})
