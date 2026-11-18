import { labelOf } from '../lib/agents';
import { AgentTile } from './AgentTile';

/**
 * Overlapping tiles of the first few agents in a row, and a "+N" pill for the
 * rest. The title names them all; write them next to it too where it matters.
 */
export function TileStack({ names, size = 30, max = 3, className }: { names: string[]; size?: number; max?: number; className?: string }) {
  const shown = names.slice(0, max);
  const more = names.length - shown.length;
  return (
    <span className={'tile-stack' + (className ? ' ' + className : '')} title={names.map(labelOf).join(', ')} style={{ ['--tile' as string]: size + 'px' }}>
      {shown.map((n) => (
        <AgentTile key={n} name={n} size={size} ring />
      ))}
      {more > 0 && (
        <span className="tile-stack-more" aria-label={more + ' more'}>
          +{more}
        </span>
      )}
    </span>
  );
}
