//基于fasthttp中的workerpool实现的协程池
//可以动态扩展协程数量，回收闲置协程

package workerpool

import (
	"sync"
	"time"
)

var (
	workpoolOnce     sync.Once
	workPoolInstance *WorkerPool
)

//WorkerPool 协程池
type WorkerPool struct {
	MaxWorkersCount       int           //最大协程数量
	MaxIdleWorkerDuration time.Duration //协程最大闲置时间

	ready []*workerChan //可用协程

	stopCh chan struct{}

	workerChanPool sync.Pool //pool复用

	lock         sync.Mutex
	workersCount int //当前工作协程数量
	mustStop     bool
}

//workerChan 为传递任务的通道
type workerChan struct {
	lastUseTime time.Time
	ch          chan BaseTask
}

//BaseTask 任务接口
type BaseTask interface {
	Run()
}

//GetWorkerPool 启动协程池并获取协程池实例
func GetWorkerPool(maxCapacity int) *WorkerPool {
	workpoolOnce.Do(func() {
		workPoolInstance = &WorkerPool{
			MaxWorkersCount: maxCapacity,
			stopCh:          make(chan struct{}),
			ready:           make([]*workerChan, 0),
		}

		//定时清理
		stopCh := workPoolInstance.stopCh
		go func() {
			var scratch []*workerChan
			for {
				workPoolInstance.clean(&scratch)
				select {
				case <-stopCh:
					return
				default:
					time.Sleep(workPoolInstance.getMaxIdleWorkerDuration())
				}
			}
		}()
	})

	return workPoolInstance
}

//Stop 停止协程池
func (wp *WorkerPool) Stop() {
	if wp.stopCh == nil {
		panic("BUG: workerPool wasn't started")
	}
	//
	close(wp.stopCh)
	wp.stopCh = nil

	//给所有协程推一个nil
	wp.lock.Lock()
	ready := wp.ready
	for i, ch := range ready {
		ch.ch <- nil
		ready[i] = nil
	}
	wp.ready = ready[:0]
	wp.mustStop = true
	wp.lock.Unlock()
}

//Work 添加新任务
func (wp *WorkerPool) Work(task BaseTask) bool {
	//获取一个协程
	ch := wp.getCh()
	if ch == nil {
		return false
	}
	//把任务推到协程开始执行
	ch.ch <- task
	return true
}

//getCh 获取一个协程（chan）
func (wp *WorkerPool) getCh() *workerChan {
	var ch *workerChan
	createWorker := false

	wp.lock.Lock()
	ready := wp.ready
	//判断可用协程的数量
	n := len(ready) - 1
	//如果没有可用的协程，则要进行创建
	if n < 0 {
		if wp.workersCount < wp.MaxWorkersCount {
			createWorker = true
			wp.workersCount++
		}
		//否则从池中取一个
	} else {
		ch = ready[n]
		ready[n] = nil
		wp.ready = ready[:n]
	}
	wp.lock.Unlock()

	//进行创建
	if ch == nil {
		//如果不需要创建则出错
		if !createWorker {
			return nil
		}
		//从sync.pool中取，复用
		vch := wp.workerChanPool.Get()
		//取不到再新建对象
		if vch == nil {
			vch = &workerChan{
				ch: make(chan BaseTask),
			}
		}
		ch = vch.(*workerChan)
		//由于是新开的协程，执行运行函数
		go func() {
			wp.workerFunc(ch)
			wp.workerChanPool.Put(vch)
		}()
	}
	return ch
}

//workerFunc 运行新的协程
func (wp *WorkerPool) workerFunc(ch *workerChan) {
	var task BaseTask

	for task = range ch.ch {
		//如果拉到nil，则结束该协程
		if task == nil {
			break
		}

		task.Run()
		task = nil
		if !wp.release(ch) {
			break
		}
	}

	wp.lock.Lock()
	wp.workersCount--
	wp.lock.Unlock()
}

//release 释放协程回到协程池
func (wp *WorkerPool) release(ch *workerChan) bool {
	ch.lastUseTime = time.Now()
	wp.lock.Lock()
	if wp.mustStop {
		wp.lock.Unlock()
		return false
	}

	wp.ready = append(wp.ready, ch)
	wp.lock.Unlock()
	return true
}

//clean 清理闲置过久的协程
func (wp *WorkerPool) clean(scratch *[]*workerChan) {
	maxIdle := wp.getMaxIdleWorkerDuration()
	currentTime := time.Now()

	//查询ready中的协程，判断其闲置时间
	wp.lock.Lock()
	ready := wp.ready
	n := len(ready)
	i := 0
	//尾部使用频率高，头部使用频率低
	for i < n && currentTime.Sub(ready[i].lastUseTime) > maxIdle {
		i++
	}
	//把未过期的放到头部
	*scratch = append((*scratch)[:0], ready[:i]...)
	if i > 0 {
		m := copy(ready, ready[i:])
		for i = m; i < n; i++ {
			ready[i] = nil
		}
		wp.ready = ready[:m]
	}
	//这里已经不会再取到废弃的协程了，所以可以提前解锁
	wp.lock.Unlock()

	//结束闲置过久的协程
	tmp := *scratch
	for i, ch := range tmp {
		ch.ch <- nil
		tmp[i] = nil
	}
}

//getMaxIdleWorkerDuration 获取最大闲置时间
func (wp *WorkerPool) getMaxIdleWorkerDuration() time.Duration {
	if wp.MaxIdleWorkerDuration <= 0 {
		return 10 * time.Second
	}
	return wp.MaxIdleWorkerDuration
}
