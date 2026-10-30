interface Edge {
  readonly label: number;
  readonly node: number;
}

interface Node<T> {
  prefix: string;
  value: T | undefined;
  children: Edge[];
}

/** Compressed index over canonical absolute paths, with component-boundary matches. */
export class PathPrefixIndex<T> {
  readonly #nodes: Node<T>[] = [{ prefix: "", value: undefined, children: [] }];

  insert(path: string, value: T): T | undefined {
    let index = 0;
    let offset = 0;
    for (;;) {
      const node = this.#nodes[index]!;
      let common = 0;
      while (common < node.prefix.length && offset + common < path.length
        && node.prefix.charCodeAt(common) === path.charCodeAt(offset + common)) common += 1;
      if (common !== node.prefix.length) {
        const suffix = { prefix: node.prefix.slice(common), value: node.value, children: node.children };
        const child = this.#nodes.length;
        this.#nodes.push(suffix);
        node.prefix = node.prefix.slice(0, common);
        node.value = undefined;
        node.children = [{ label: suffix.prefix.charCodeAt(0), node: child }];
      }
      offset += common;
      if (offset === path.length) {
        const previous = node.value;
        node.value = value;
        return previous;
      }
      const label = path.charCodeAt(offset);
      const position = lowerBound(node.children, label);
      const edge = node.children[position];
      if (edge?.label === label) {
        index = edge.node;
      } else {
        node.children.splice(position, 0, { label, node: this.#nodes.length });
        this.#nodes.push({ prefix: path.slice(offset), value, children: [] });
        return undefined;
      }
    }
  }

  longest(path: string): T | undefined {
    let index = 0;
    let offset = 0;
    let selected: T | undefined;
    for (;;) {
      const node = this.#nodes[index]!;
      for (let part = 0; part < node.prefix.length; part += 1) {
        if (node.prefix.charCodeAt(part) !== path.charCodeAt(offset + part)) return selected;
      }
      offset += node.prefix.length;
      if (node.value !== undefined
        && (offset === 1 || offset === path.length || path.charCodeAt(offset) === 47)) selected = node.value;
      if (offset === path.length) return selected;
      const label = path.charCodeAt(offset);
      const edge = node.children[lowerBound(node.children, label)];
      if (edge?.label !== label) return selected;
      index = edge.node;
    }
  }
}

// UTF-16 labels have at most 65,536 alternatives: at most 17 comparisons per edge.
function lowerBound(children: readonly Edge[], label: number): number {
  let first = 0;
  let last = children.length;
  while (first < last) {
    const middle = first + Math.floor((last - first) / 2);
    if (children[middle]!.label < label) first = middle + 1;
    else last = middle;
  }
  return first;
}
