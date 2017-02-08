using System;
using System.Collections;
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
        /// <summary>
        /// Инициализация архиватора
        /// </summary>
        /// <param name="srcFile">исходный файл</param>
        /// <param name="dstFile">конечный файл</param>
        /// <param name="compress">режим работы</param>
        public Archivator(string srcFile, string dstFile, bool compress)
        {
            SrcFile = srcFile;
            DstFile = dstFile;
            Compress = compress;
        }
/*
        public void Compress()
        {
            Start(DstFile, true);
            try
            {
                var fileStream = new FileStream(SrcFile, FileMode.Open, FileAccess.Read, FileShare.Read);                
                {
                    int count = 0;
                    do
                    {
                        lock (_threads)
                        {
                            if (_threads.Count == _numberOfProcessors) continue;
                        }
                        long availableBlocks;
                        lock (_blocksLock)
                        {
                            availableBlocks = _maxBlocks - (2 * _readedBlocks - _processedBlocks - _writedBlocks); // количество доступных блоков оперативной памяти
                            if (availableBlocks <= 2) continue;
                            _numBlocks *= 2;
                        }
                        if (_numBlocks * _blockSize > _maxBufferSize) _numBlocks = _maxBufferSize / _blockSize;
                        if (2 * _numBlocks > availableBlocks) _numBlocks = (int)availableBlocks / 2;
                        var bufferSize = _numBlocks * _blockSize;

                        if (bufferSize > fileStream.Length - fileStream.Position)
                            bufferSize = (int)(fileStream.Length - fileStream.Position);
                        var byteArray = new byte[bufferSize];
                        count = fileStream.Read(byteArray, 0, bufferSize);
                        if (count == 0) break;
                        var thread = new Thread(CompressPart) { Name = "thread" + _partsNumber};
                        lock (_threads)
                        {
                            _threads.Add(thread);
                        }
                        _readedBlocks += _numBlocks;
                        thread.Start(new FilePart(_partsNumber, new MemoryStream(byteArray), _numBlocks));
                        _partsNumber++;
                        if (Abort || ErrorOccured) return;
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
*/
        /// <summary>
        /// Получение информации о железе, запуск пула основных потоков и потока записи в конечный файл
        /// </summary>
        public void Start()
        {            
            _threads = new Thread[Environment.ProcessorCount];
            _readStream = new FileStream(SrcFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (!Compress)
            {
                _metaInfo = GetMetaInfo(_readStream);
                if (_metaInfo == null) return;
            }
            
            PerformanceCounter ramCounter = new PerformanceCounter("Memory", "Available MBytes");
            _maxBlocks = (long)(ramCounter.NextValue() * 1024 * 1024) / _blockSize / 2;
            var writeThread = Compress ? new Thread(WriteToArchive) : new Thread(WriteFromArchive);
            writeThread.Start(DstFile);
            
            for (int i = 0; i < _threads.Length; i++)
            {
                if (Compress) _threads[i] = new Thread(CompressPart);
                else _threads[i] = new Thread(DecompressPart);
                _threads[i].Start(_readStream);
            }
            
        }

        private Queue<FilePartInfo> GetMetaInfo(FileStream fileStream)
        {
            Queue<FilePartInfo> metaInfo = null;
            try
            {

                var metalength = new byte[4];
                fileStream.Position = fileStream.Length - 4;
                fileStream.Read(metalength, 0, 4);
                var length = BitConverter.ToInt32(metalength, 0);
                _readStreamEndPosition = fileStream.Length - length - 4;
                var byteArray = new byte[length];
                fileStream.Position = _readStreamEndPosition;
                fileStream.Read(byteArray, 0, length);
            }
            catch (OutOfMemoryException)
            {
                Console.WriteLine("Недостаточно памяти для завершения операции");
                lock (ResultLock)
                {
                    ErrorOccured = true;
                }
                return null;
            }
            catch (IOException e)
            {
                Console.WriteLine("Ошибка ввода/вывода " + e.Message);
                lock (ResultLock)
                {
                    ErrorOccured = true;
                }
                return null;
            }
            catch (Exception e)
            {
                UnexpectedError(e);
                return null;
            }
            return metaInfo;
        }
/*
        public void Decompress()
        {
            Start(DstFile, false);
            try
            {
                using (var fileStream = new FileStream(SrcFile, FileMode.Open))
                {
                    var metalength = new byte[4];
                    fileStream.Position = fileStream.Length - 4;
                    fileStream.Read(metalength, 0, 4);
                    var length = BitConverter.ToInt32(metalength, 0);
                    var metaInfo = new byte[length];
                    fileStream.Position = fileStream.Length - length - 4;
                    fileStream.Read(metaInfo, 0, length);
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
                            ErrorOccured = true;
                            return;
                        }
                    }
                    foreach (var partInfo in _metaInfo.OrderBy(i => i.Index)) // Сортировка для того, чтобы быть уверенным, 
                                                                              // что в очереди всегда будет появляться нужный кусок и тогда можно ограничить размер очереди
                    {
                        bool readyForNewThread;
                        do
                        {
                            lock (_threads)
                            {
                                readyForNewThread = _threads.Count < _numberOfProcessors;
                            }
                            lock (_queue)
                            {
                                readyForNewThread = _queue.Count < 2*_numberOfProcessors && readyForNewThread;
                            }
                        } while (!readyForNewThread);
                        var buffer = new byte[partInfo.Size];
                        fileStream.Position = partInfo.Offset;
                        fileStream.Read(buffer, 0, buffer.Length);
                        var part = new FilePart(partInfo.Index, new MemoryStream(buffer), partInfo.Size / _blockSize);
                        var thread = new Thread(DecompressPart) {Name = "thread" + part.Index};
                        lock (_threads)
                        {
                            _threads.Add(thread);
                        }
                        _readedBlocks += _numBlocks;
                        thread.Start(part);
                        if (Abort || ErrorOccured) return;
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
                ErrorOccured = true;
            }
            catch (Exception e)
            {
                UnexpectedError(e);
            }
        }
*/
        private Queue<FilePart> _queue = new Queue<FilePart>();

        private void CompressPart()
        {
            try
            {
                long availableBlocks;
                lock (_blocksLock)
                {
                    availableBlocks = _maxBlocks - (2 * _readedBlocks - _processedBlocks - _writedBlocks); // количество доступных блоков оперативной памяти
                    if (availableBlocks <= 2) continue;
                    _numBlocks *= 2;
                }
                if (_numBlocks * _blockSize > _maxBufferSize) _numBlocks = _maxBufferSize / _blockSize;
                if (2 * _numBlocks > availableBlocks) _numBlocks = (int)availableBlocks / 2;
                var bufferSize = _numBlocks * _blockSize;

                if (bufferSize > fileStream.Length - fileStream.Position)
                    bufferSize = (int)(fileStream.Length - fileStream.Position);
                var byteArray = new byte[bufferSize];
                count = fileStream.Read(byteArray, 0, bufferSize);
                if (count == 0) break;
                var thread = new Thread(CompressPart) { Name = "thread" + _partsNumber };
                lock (_threads)
                {
                    _threads.Add(thread);
                }
                _readedBlocks += _numBlocks;
                thread.Start(new FilePart(_partsNumber, new MemoryStream(byteArray), _numBlocks));
                _partsNumber++;
                if (Abort || ErrorOccured) return;



                var memStream = new MemoryStream();
                using (var zipStream = new GZipStream(memStream, CompressionMode.Compress, true))
                {
                    WriteFromStreamToStream(part.Stream, zipStream);
                    part.Dispose();
                }
                lock (_queue)
                {
                    _queue.Enqueue(new FilePart(part.Index, memStream, part.Blocks));
                }
                lock (_blocksLock)
                {
                    _processedBlocks += part.Blocks;
                }
                lock (_threads)
                {
                    var thread = _threads.FirstOrDefault(t => t.Name != null && t.Name.Equals("thread" + part.Index));
                    _threads.Remove(thread);
                }   
            }
            catch (Exception e)
            {
                UnexpectedError(e);
            }
        }

        private void DecompressPart(object dataToDecompress)
        {
            var part = dataToDecompress as FilePart;
            if (part == null) return;
            try
            {
                var memStream = new MemoryStream();
                using (var zipStream = new GZipStream(part.Stream, CompressionMode.Decompress, true))
                {
                    WriteFromStreamToStream(zipStream, memStream);
                    part.Stream.Dispose();
                }
                lock (_queue)
                {
                    _queue.Enqueue(new FilePart(part.Index, memStream, part.Blocks));
                }
                lock (_blocksLock)
                {
                    _processedBlocks += part.Blocks;
                }
                lock (_threads)
                {
                    _threads.Remove(_threads.FirstOrDefault(t => t.Name != null && t.Name.Equals("thread" + part.Index)));
                }
            }
            catch (Exception e)
            {
                UnexpectedError(e);
            }

        }

        private void UnexpectedError(Exception e)
        {
            Console.WriteLine("Непредвиденная ошибка: " + e.Message);
            lock (ResultLock)
            {
                ErrorOccured = true;
            }
        }

        private Queue<FilePartInfo> _metaInfo = new Queue<FilePartInfo>();

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
                            _metaInfo.Enqueue(new FilePartInfo(part.Index, writeStream.Position, part.Stream.Length));
                            WriteFromStreamToStream(part.Stream, writeStream);
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
                                    !_threads.Any(t => t != null && (t.ThreadState & ThreadState.Stopped) == 0))
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
                        var length = BitConverter.GetBytes(byteArray.Length);
                        writeStream.Write(length, 0, 4);
                    }
                    Console.WriteLine("Архивация завершена");
                    WorkCompleted = true;
                    GC.Collect();
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Файл назначения не найден");
                lock (ResultLock)
                {
                    ErrorOccured = true;
                }
            }
            catch (OutOfMemoryException)
            {
                Console.WriteLine("Недостаточно памяти для завершения операции");
                lock (ResultLock)
                {
                    ErrorOccured = true;
                }
            }
            catch (Exception e)
            {
                UnexpectedError(e);
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
                                WriteFromStreamToStream(part.Stream, writeStream);
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
                                    !_threads.Any(t => t != null && (t.ThreadState & ThreadState.Stopped) == 0))
                                {
                                    endWrite = true;
                                }
                            }
                        if (Abort || ErrorOccured) return;
                    } while (!endWrite);
                    Console.WriteLine("Разархивация завершена");
                    lock (ResultLock)
                    {
                        WorkCompleted = true;
                    }
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Файл назначения не найден");
                lock (ResultLock)
                {
                    ErrorOccured = true;
                }
            }
            catch (Exception e)
            {
                UnexpectedError(e);
            }
        }

        /// <summary>
        /// Запись из потока в поток
        /// </summary>
        /// <param name="srcStream">исходный поток</param>
        /// <param name="dstStream">поток назначения</param>
        private void WriteFromStreamToStream(Stream srcStream, Stream dstStream)
        {
            try
            {
                var buffer = new byte[4096];
                int count;
                do
                {
                    count = srcStream.Read(buffer, 0, buffer.Length);
                    dstStream.Write(buffer, 0, count);
                } while (count > 0);
            }
            catch (OutOfMemoryException)
            {
                Console.WriteLine("Недостаточно памяти для завершения операции");
                lock (ResultLock)
                {
                    ErrorOccured = true;
                }
            }
            catch (Exception e)
            {
                UnexpectedError(e);
            }
        }

        
        private string DstFile { get; }
        private string SrcFile { get; }
        private bool Compress { get; }

        public bool ErrorOccured { get; private set; }
        public bool WorkCompleted { get; private set; }
        public bool Abort { get; set; }

        private Thread[] _threads;
        private long _partsNumber;

        public object ResultLock { get; private set; } = new object();
        private object _readEndLock = new object();
        private bool _readEnded;

        private int _blockSize = 1024*1024;
        private int _numBlocks = 1;
        private long _maxBlocks;
        private int _maxBufferSize = 1024*1024*1024; 

        private long _readedBlocks;
        private long _processedBlocks;
        private long _writedBlocks;

        private object _blocksLock = new object();

        private FileStream _readStream;
        private long _readStreamEndPosition;
    }
}
