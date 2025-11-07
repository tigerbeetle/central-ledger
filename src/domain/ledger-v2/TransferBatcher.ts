import { logger } from '../../shared/logger';
import { Client, Transfer } from 'tigerbeetle-node';

interface QueueItem {
  transfer: Transfer;
  resolve: (error?: number) => void;
}

/**
 * Promise based queue to let us efficently batch together transfers
 */
export class TransferBatcher {
  private _client: Client;
  private _transferQueue: QueueItem[] = [];
  private _batchSize: number;
  private _batchInterval: number;
  private _timer: NodeJS.Timeout | null = null;

  constructor(client: Client, batchSize: number, batchInterval: number) {
    this._client = client;
    this._batchSize = batchSize;
    this._batchInterval = batchInterval;

    this._timer = setInterval(() => this.flushQueue(), this._batchInterval);
  }

  cleanup(): void {
    if (!this._timer) {
      return
    }
    
    clearInterval(this._timer)
  }

  async enqueueTransfer(transfer: Transfer): Promise<undefined | number> {
    return new Promise((resolve) => {
      this._transferQueue.push({ transfer, resolve});

      if (this._transferQueue.length >= this._batchSize) {
        logger.info(`TransferBatcher.enqueueTransfer() reached queue length of: ${this._transferQueue.length} - shipping transfers`)
        this.flushQueue();
      }
    });
  }

  async flushQueue(): Promise<void> {
    if (this._transferQueue.length === 0) {
      return;
    }

    const batch = this._transferQueue.splice(0, this._batchSize);
    logger.info(`TransferBatcher.enqueueTransfer() shipping batch of size: ${batch.length} - to TigerBeetle`)
  
    const errors = await this._client.createTransfers(batch.map(t => t.transfer));
    
    const errorIndices: Record<number, number> = errors.reduce((acc, curr) => {
      acc[curr.index] = curr.result;
      return acc;
    }, {} as Record<number, number>);

    batch.forEach((item, idx) => {
      if (errorIndices[idx]) {
        item.resolve(errorIndices[idx]);
      } else {
        item.resolve();
      }
    });
  }
}