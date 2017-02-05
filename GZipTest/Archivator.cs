using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using ThreadState = System.Threading.ThreadState;


namespace GZipTest
{
    public class Archivator
    {
        public Archivator(string srcFile, string dstFile)
        {
            SrcFile = srcFile;
            DstFile = dstFile;
        }

        public void Compress()
        {
            Initialize(DstFile, true);
            try
            {
                using (var fileStream = new FileStream(SrcFile, FileMode.Open, FileAccess.Read))
                {
                    int count = 0;
                    do
                    {
                        lock (threads)
                        {
                            if (threads.Count == _numberOfProcessors) continue;
                        }
                        long availableBlocks;
                        lock (_blocksLock)
                        {
                            availableBlocks = _maxBlocks - (2 * _readedBlocks - _processedBlocks - _writedBlocks); // количество доступных блоков оперативной памяти
                            if (availableBlocks <= 2) continue;
                            if (_writedBlocks != 0 && _processedBlocks >= _writedBlocks) _numBlocks *= 2;
                            if (_writedBlocks != 0 && _processedBlocks <= 0.5 * _writedBlocks && _numBlocks > 1) _numBlocks /= 2;
                        }
                        if (_numBlocks * _blockSize > _maxBuferSize) _numBlocks = _maxBuferSize / _blockSize;
                        if (2 * _numBlocks > availableBlocks) _numBlocks = (int)availableBlocks / 2;
                        var _bufferSize = _numBlocks * _blockSize;

                        if (_bufferSize > fileStream.Length - fileStream.Position)
                            _bufferSize = (int)(fileStream.Length - fileStream.Position);
                        var byteArray = new byte[_bufferSize];
                        count = fileStream.Read(byteArray, 0, _bufferSize);
                        if (count == 0) break;
                        var thread = new Thread(CompressPart) { Name = "thread" + partsNumber };
                        lock (threads)
                        {
                            threads.Add(thread);
                        }
                        _readedBlocks += _numBlocks;
                        thread.Start(new FilePart(partsNumber, new MemoryStream(byteArray), _numBlocks));
                        partsNumber++;
                    } while (count > 0);
                }
                lock (_readEndLock)
                {
                    _readEnded = true;
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Файл " + SrcFile + " не найден");
                lock (ResultLock)
                {
                    ErrorOccured = true;
                }
                ErrorOccured = true;
            }
            catch (OutOfMemoryException)
            {
                _numBlocks /= 2;
            }
        }

        private void Initialize(string dstFile, bool compress)
        {
            _numberOfProcessors = Environment.ProcessorCount;
            PerformanceCounter ramCounter = new PerformanceCounter("Memory", "Available MBytes");
            _maxBlocks = (long) (ramCounter.NextValue() * 1024 * 1024) / _blockSize / 2;
            var writeThread = compress ? new Thread(WriteToArchive) : new Thread(WriteFromArchive);
            writeThread.Start(dstFile);
        }


        public void Decompress()
        {
            Initialize(DstFile, false);
            try
            {
                using (var fileStream = new FileStream(SrcFile, FileMode.Open))
                {
                    var metaLenght = new byte[4];
                    fileStream.Position = fileStream.Length - 4;
                    fileStream.Read(metaLenght, 0, 4);
                    var lenght = BitConverter.ToInt32(metaLenght, 0);
                    var metaInfo = new byte[lenght];
                    fileStream.Position = fileStream.Length - lenght - 4;
                    fileStream.Read(metaInfo, 0, lenght);
                    using (var memStream = new MemoryStream(metaInfo))
                    {
                        var serializer = new BinaryFormatter();
                        try
                        {
                            _metaInfo = (List<FilePartInfo>) serializer.Deserialize(memStream);
                        }
                        catch (SerializationException)
                        {
                            Console.WriteLine("Данный файл не является архивом подходящего формата");
                        }
                    }
                    foreach (var partInfo in _metaInfo)
                    {
                        var freeThread = false;
                        do
                        {
                            lock (threads)
                            {
                                freeThread = threads.Count < _numberOfProcessors;
                            }
                        } while (!freeThread);
                        var buffer = new byte[partInfo.Size];
                        fileStream.Position = partInfo.Offset;
                        fileStream.Read(buffer, 0, buffer.Length);
                        var part = new FilePart(partInfo.Index, new MemoryStream(buffer), partInfo.Size/_blockSize);
                        var thread = new Thread(DecompressPart) { Name = "thread" + part.Index };
                        lock (threads)
                        {
                            threads.Add(thread);
                        }
                        _readedBlocks += _numBlocks;
                        thread.Start(part);
                    }
                    lock (_readEndLock)
                    {
                        _readEnded = true;
                    }
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Исходный файл не найден");
            }
        }

        private Queue<FilePart> _queue = new Queue<FilePart>();

        private void CompressPart(object dataToCompress)
        {
            var part = dataToCompress as FilePart;
            if (part == null) return;
            var memStream = new MemoryStream();
            using (var zipStream = new GZipStream(memStream, CompressionMode.Compress, true))
            {
                CopyStreamToStream(part.Stream, zipStream, 0);
                part.Stream.Dispose();
            }
            lock (_queue)
            {
                _queue.Enqueue(new FilePart(part.Index, memStream, part.Blocks));
            }
            Interlocked.Increment(ref _queueSize);
            lock (_blocksLock)
            {
                _processedBlocks += part.Blocks;
            }
            lock (threads)
            {
                threads.Remove(threads.FirstOrDefault(t => t.Name != null && t.Name.Equals("thread" + part.Index)));
            }
        }

        private void DecompressPart(object dataToDecompress)
        {
            var part = dataToDecompress as FilePart;
            if (part == null) return;
            var memStream = new MemoryStream();
            using (var zipStream = new GZipStream(part.Stream, CompressionMode.Decompress, true))
            {
                CopyStreamToStream(zipStream, memStream, 0);
                part.Stream.Dispose();
            }
            lock (_queue)
            {
                _queue.Enqueue(new FilePart(part.Index, memStream, part.Blocks));
            }
            Interlocked.Increment(ref _queueSize);
            lock (_blocksLock)
            {
                _processedBlocks += part.Blocks;
            }
            lock (threads)
            {
                threads.Remove(threads.FirstOrDefault(t => t.Name != null && t.Name.Equals("thread" + part.Index)));
            }
        }

        private List<FilePartInfo> _metaInfo = new List<FilePartInfo>();

        private void WriteToArchive(object dstFile)
        {
            var file = dstFile.ToString();
            try
            {
                using (var writeStream = new FileStream(file, FileMode.Create, FileAccess.Write))
                {
                    bool endWrite = false;
                    do
                    {
                        FilePart part = null;
                        lock (_queue)
                        {
                            if (_queue.Count > 0)
                                part = _queue.Dequeue();
                        }
                        if (part != null)
                        {
                            _metaInfo.Add(new FilePartInfo(part.Index, writeStream.Position, part.Stream.Length));
                            CopyStreamToStream(part.Stream, writeStream, writeStream.Position);
                            part.Stream.Dispose();
                            lock (_blocksLock)
                            {
                                _writedBlocks += part.Blocks;
                            }
                        }
                        else 
                        lock (_readEndLock)
                        {
                            if (_readEnded &&
                                !threads.Exists(t => t != null && (t.ThreadState & ThreadState.Stopped) == 0))
                            {
                                endWrite = true;
                            }
                        }
                    } while (!endWrite);
                    using (var memStream = new MemoryStream())
                        {
                            var formatter = new BinaryFormatter();
                            formatter.Serialize(memStream, _metaInfo);
                            var byteArray = memStream.ToArray();
                            writeStream.Write(byteArray, 0, byteArray.Length);
                            var lenght = new byte[4];
                            lenght = BitConverter.GetBytes(byteArray.Length);
                            writeStream.Write(lenght, 0, 4);
                        }
                    Console.WriteLine("Архивация завершена");
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Файл назначения не найден");
            }
        }

        private void WriteFromArchive(object dstFile)
        {
            var file = dstFile.ToString();
            long index = 0;
            Queue<FilePart> tempQueue = new Queue<FilePart>();
            List<long> tempIndexes = new List<long>();
            try
            {
                using (var writeStream = new FileStream(file, FileMode.Create, FileAccess.Write))
                {
                    bool endWrite = false;
                    do
                    {
                        FilePart part = null;
                        if (tempQueue.Count > 0 && tempIndexes.Contains(index))
                        {
                            do 
                            {
                                part = tempQueue.Dequeue();
                                if (part.Index != index) tempQueue.Enqueue(part);
                            } while(part.Index != index);
                            tempIndexes.Remove(index);
                        }
                        else
                        {
                            lock (_queue)
                            {
                                if (_queue.Count > 0)
                                    part = _queue.Dequeue();
                            }
                        }
                        if (part != null)
                        {
                            if (part.Index == index)
                            {
                                CopyStreamToStream(part.Stream, writeStream, writeStream.Position);
                                part.Stream.Dispose();
                                lock (_blocksLock)
                                {
                                    _writedBlocks += part.Blocks;
                                }
                                index++;
                            }
                            else
                            {
                                tempQueue.Enqueue(part);
                                tempIndexes.Add(part.Index);
                            }
                        }
                        else
                            lock (_readEndLock)
                            {
                                if (_readEnded &&
                                    !threads.Exists(t => t != null && (t.ThreadState & ThreadState.Stopped) == 0))
                                {
                                    endWrite = true;
                                }
                            }
                    } while (!endWrite || !Abort || !ErrorOccured);
                    Console.WriteLine("Разархивация завершена");
                    WorkCompleted = true;
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Файл назначения не найден");
                ErrorOccured = true;
            }
        }

        private void CopyStreamToStream(Stream srcStream, Stream dstStream, long dstPosition)
        {
            var buffer = new byte[4096];
            if (dstStream is FileStream) dstStream.Position = dstPosition;
            int count;
            do
            {
                count = srcStream.Read(buffer, 0, buffer.Length);
                dstStream.Write(buffer, 0, count);
            } while (count > 0);
        }

        private string DstFile { get; set; }
        private string SrcFile { get; set; }
        private bool IsCompress { get; set; }

        public bool ErrorOccured { get; private set; }
        public bool WorkCompleted { get; private set; }
        public bool Abort { get; set; }
        private List<Thread> threads = new List<Thread>();
        private long partsNumber;

        public object ResultLock { get; private set; } = new object();
        private object _readEndLock = new object();
        private bool _readEnded;

        private int _queueSize;
        private int _blockSize = 1024*1024;
        private int _numBlocks = 1;
        private long _maxBlocks;
        private int _maxBuferSize = 1024*1024*1024; 

        private long _readedBlocks;
        private long _processedBlocks;
        private long _writedBlocks;

        private int _numberOfProcessors;

        private object _blocksLock = new object();
    }
}
