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

        /// <summary>
        /// Получение информации о железе, запуск пула основных потоков и потока записи в конечный файл
        /// </summary>
        public void Start()
        {
            try
            {
                _numProcessors = Environment.ProcessorCount;
                if (!Compress)
                {
                    using (var fileStream = new FileStream(SrcFile, FileMode.Open, FileAccess.Read))
                    {
                        var metalength = new byte[4];
                        fileStream.Position = fileStream.Length - 4;
                        fileStream.Read(metalength, 0, 4);
                        var length = BitConverter.ToInt32(metalength, 0);
                        var byteArray = new byte[length];
                        fileStream.Position = fileStream.Length - length - 4;
                        fileStream.Read(byteArray, 0, length);
                        using (var memStream = new MemoryStream(byteArray))
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
                                fileStream.Dispose();
                                memStream.Dispose();
                                return;
                                
                            }
                            catch (Exception e)
                            {
                                UnexpectedError(e);
                                return;
                            }
                        }
                    }
                }
                PerformanceCounter ramCounter = new PerformanceCounter("Memory", "Available MBytes");
                _availableBlocks = (long)(ramCounter.NextValue() * 1024 * 1024) / BlockSize / 2;
                var writeThread = Compress ? new Thread(WriteToArchive) : new Thread(WriteFromArchive);
                writeThread.Start(DstFile);
                _threads = new Thread[_numProcessors];
                lock (_threads)
                {
                    for (int i = 0; i < _threads.Length; i++)
                    {
                        if (Compress) _threads[i] = new Thread(CompressPart);
                        else _threads[i] = new Thread(DecompressPart);
                        _threads[i].Start();
                    }
                }
            }
            catch (Exception e)
            {
                UnexpectedError(e);
            }
            
        }

        private Queue<FilePart> _queue = new Queue<FilePart>();

        private void CompressPart()
        {
            try
            {
                using (var fileStream = new FileStream(SrcFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    // По спецификации чтение и запись булевских переменных - атомарны
                    while (!_readEnded && !Abort && !ErrorOccured && !WorkCompleted)
                    {
                        int bufferSize;
                        int numBlocks;
                        long readPosition;
                        long partIndex;
                        lock (_blocksLock)
                        {
                            if (_availableBlocks <= 2) continue;
                            _numBlocks = 2 * _numBlocks;
                            if (2*_numBlocks > _availableBlocks) _numBlocks = (int)(_availableBlocks/2);
                            if (_numBlocks*BlockSize > MaxBufferSize) _numBlocks = MaxBufferSize/BlockSize;
                            bufferSize = _numBlocks * BlockSize;
                            if (bufferSize > fileStream.Length - _currenPosition)
                            {
                                bufferSize = (int)(fileStream.Length - _currenPosition);
                                _numBlocks = bufferSize / _numBlocks;
                                _readEnded = true;
                            }
                            readPosition = _currenPosition;
                            partIndex = _currentPartIndex;
                            _currentPartIndex++;
                            _currenPosition = _currenPosition + bufferSize;
                            numBlocks = _numBlocks;
                            _availableBlocks -= 2 * numBlocks;
                        }
                        var byteArray = new byte[bufferSize];
                        fileStream.Position = readPosition;
                        fileStream.Read(byteArray, 0, bufferSize);                        
                        var memStream = new MemoryStream();
                        using (var zipStream = new GZipStream(memStream, CompressionMode.Compress, true))
                        {
                            zipStream.Write(byteArray, 0, bufferSize);
                        }
                        lock (_queue)
                        {
                            _queue.Enqueue(new FilePart(partIndex, memStream, numBlocks));
                        }
                        lock (_blocksLock)
                        {
                            _availableBlocks += numBlocks;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                UnexpectedError(e);
            }
        }
                    

        private void DecompressPart()
        {
            try
            {
                using (var fileStream = new FileStream(SrcFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    while (!_readEnded && !Abort && !ErrorOccured && !WorkCompleted)
                    {
                        lock (_queue)
                        {
                            if (_queue.Count > 2*_numProcessors) continue;
                        }
                        FilePartInfo info;
                        int bufferSize;
                        long position;
                        int numBlocks;
                        lock (_metaInfo)
                        {
                            info = _metaInfo.FirstOrDefault(m => m.Index == _currentPartIndex);
                            if (info == null)
                            {
                                _readEnded = true;
                                continue;
                            }
                            Interlocked.Increment(ref _currentPartIndex);
                            bufferSize = (int) info.Size;
                            position = info.Offset;
                            numBlocks = bufferSize/BlockSize;
                        }
                        var byteArray = new byte[bufferSize];
                        fileStream.Position = position;
                        fileStream.Read(byteArray, 0, bufferSize);
                        var memStream = new MemoryStream();
                        using (var readStream = new MemoryStream(byteArray))
                        {
                            using (var zipStream = new GZipStream(readStream, CompressionMode.Decompress, true))
                            {
                                WriteFromStreamToStream(zipStream, memStream);
                            }
                        }
                        lock (_queue)
                        {
                            _queue.Enqueue(new FilePart(info.Index, memStream, numBlocks));
                        }
                    }
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Исходный файл не найден");
                ErrorOccured = true;
            }
            catch (IOException)
            {
                Console.WriteLine("Произошла ошибка ввода/вывода");
                ErrorOccured = true;
            }
            catch (Exception e)
            {
                UnexpectedError(e);
            }
        }

        private void UnexpectedError(Exception e)
        {
            Console.WriteLine("Непредвиденная ошибка: " + e.Message);
            ErrorOccured = true;
            
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
                            WriteFromStreamToStream(part.Stream, writeStream);
                            part.Stream.Dispose();
                            lock (_blocksLock)
                            {
                                _availableBlocks += part.Blocks;
                            }
                        }
                        else
                            lock (_threads)
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
                        var serializer = new BinaryFormatter();
                        serializer.Serialize(memStream, _metaInfo);
                        var byteArray = memStream.ToArray();
                        writeStream.Write(byteArray, 0, byteArray.Length);
                        var length = BitConverter.GetBytes(byteArray.Length);
                        writeStream.Write(length, 0, 4);
                    }
                    Console.WriteLine("Архивация завершена");
                    WorkCompleted = true;
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Файл назначения не найден");
                ErrorOccured = true;
            }
            catch (OutOfMemoryException)
            {
                Console.WriteLine("Недостаточно памяти для завершения операции");
                ErrorOccured = true;
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
                                index++;
                            }
                            else
                            {
                                tempQueue.Enqueue(part);
                                tempIndexes.Add(part.Index);
                            }
                        }
                        else
                            lock (_threads)
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
                    WorkCompleted = true;
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Файл назначения не найден");
                ErrorOccured = true;
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
                ErrorOccured = true;
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

        private int _numProcessors;
        private Thread[] _threads;
        private long _currentPartIndex;

        private bool _readEnded;

        private const int BlockSize = 1024*1024;
        private int _numBlocks = 1;
        private long _availableBlocks;
        private const int MaxBufferSize = 1024*1024*1024;

        private readonly object _blocksLock = new object();

        private long _currenPosition;
    }
}
