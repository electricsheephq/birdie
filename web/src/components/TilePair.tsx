import { AgentTile } from './AgentTile';

/**
 * Two agents' tiles stacked corner to corner, the first in front: who sent a
 * message and who got it, or the two sides of a thread. With one name it's a
 * single tile. Decorative: write the names next to it.
 */
export function TilePair({ a, b, more = 0, size = 34, className }: { a: string; b?: string; more?: number; size?: number; className?: string }) {
  const pill = b && more > 0;
  return (
    <div
      className={'tile-pair tile-pair-stack' + (className ? ' ' + className : '')}
      style={{ width: b ? Math.round(size * 1.76) : size, ['--tile' as string]: size + 'px' }}
    >
      <AgentTile name={a} size={size} ring />
      {b && <AgentTile name={b} size={size} ring style={{ marginLeft: -Math.round(size * 0.24), marginTop: Math.round(size * 0.3) }} />}
      {/* More agents than the two drawn: a pill over the second tile's corner. */}
      {pill && (
        <span className="tile-pair-more" aria-label={more + ' more'}>
          +{more}
        </span>
      )}
    </div>
  );
}
