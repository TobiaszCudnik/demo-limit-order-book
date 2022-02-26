package lob

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/bitmark-inc/bitmarkd/avl"
)

type Order struct {
	ID       int
	UID      int
	Price    int
	Quantity int
	IsSell   bool

	NextOrder   *Order
	PrevOrder   *Order
	ParentLimit *Limit
}

type Limit struct {
	Price       int
	Size        int
	TotalVolume int

	HeadOrder *Order
	TailOrder *Order
}

type LimitOrderBook struct {
	BuyTree  *avl.Tree
	SellTree *avl.Tree

	LowestSell *Order
	HighestBuy *Order

	OrdersByID map[int]*Order

	Lock sync.RWMutex
}

func NewLimitOrderBook() *LimitOrderBook {
	return &LimitOrderBook{
		BuyTree:    avl.New(),
		SellTree:   avl.New(),
		OrdersByID: make(map[int]*Order),
		Lock:       sync.RWMutex{},
	}
}

func (lob *LimitOrderBook) CheckBookCrossed(o *Order) bool {
	if o.IsSell && lob.HighestBuy != nil && o.Price <= lob.HighestBuy.Price {
		return true
	} else if !o.IsSell && lob.LowestSell != nil && o.Price >= lob.LowestSell.Price {
		return true
	}
	return false
}

func (lob *LimitOrderBook) NewOrder(uid, price, qty int, isSell bool, id int) ([]string, error) {
	// aquire the write lock
	lob.Lock.Lock()
	// new order struct
	o := &Order{
		ID:       id,
		UID:      uid,
		Price:    price,
		Quantity: qty,
		IsSell:   isSell,
	}
	if lob.CheckBookCrossed(o) {
		// reject crossing orders
		lob.Lock.Unlock()
		return []string{lob.getOrderOutput(false, o)}, nil
	}
	tree := lob.getOrderTree(o)
	// get the price limit
	key := treeKey{price}
	var limit *Limit
	node, _ := tree.Search(key)
	if node == nil {
		// create a new price limit node
		limit = &Limit{
			Price:       price,
			Size:        1,
			TotalVolume: o.Quantity,
			HeadOrder:   o,
			TailOrder:   o,
		}
		tree.Insert(key, limit)
	} else {
		// update the existing limit
		limit = node.Value().(*Limit)
		limit.TailOrder.NextOrder = o
		limit.TailOrder = o
		limit.Size++
		limit.TotalVolume += o.Quantity
	}
	o.ParentLimit = limit
	// store in the order index
	lob.OrdersByID[id] = o
	var output = []string{lob.getOrderOutput(true, o)}
	// update min/max indexes, check ToB
	output = lob.newOrderIndexes(output, o)
	// release, return
	lob.Lock.Unlock()
	return output, nil
}

func (lob *LimitOrderBook) CancelOrder(id int) ([]string, error) {
	// aquire the write lock
	lob.Lock.Lock()
	o, ok := lob.OrdersByID[id]
	if !ok {
		return nil, fmt.Errorf("Order %d doesnt exist", id)
	}
	tobChanged := lob.cancelOrderIndexes(o)
	// dispose
	delete(lob.OrdersByID, id)
	// cancellation accepted
	var output = []string{lob.getOrderOutput(true, o)}
	// check ToB
	if tobChanged {
		output = append(output, lob.getTobOutput(o.IsSell))
	}
	// release, return
	lob.Lock.Unlock()
	return output, nil
}

func (lob *LimitOrderBook) newOrderIndexes(output []string, o *Order) []string {
	if o.IsSell {
		if lob.LowestSell == nil || o.Price < lob.LowestSell.Price {
			// price changed
			lob.LowestSell = o
			output = append(output, lob.getTobOutput(o.IsSell))
		} else if o.Price == lob.LowestSell.Price {
			// quantity changed
			output = append(output, lob.getTobOutput(o.IsSell))
		}
	} else {
		if lob.HighestBuy == nil || o.Price > lob.HighestBuy.Price {
			// price changed
			lob.HighestBuy = o
			output = append(output, lob.getTobOutput(o.IsSell))
		} else if o.Price == lob.HighestBuy.Price {
			// quantity changed
			output = append(output, lob.getTobOutput(o.IsSell))
		}
	}
	return output
}

func (lob *LimitOrderBook) cancelOrderIndexes(o *Order) bool {
	tobChanged := false
	tree := lob.getOrderTree(o)
	limit := o.ParentLimit
	key := treeKey{limit.Price}
	node, _ := tree.Search(key)
	if limit.Size == 1 {
		// update min/max indexes
		if o == lob.LowestSell {
			// update lob.LowestSell
			higherSellNode := node.Next()
			if higherSellNode != nil {
				limit := higherSellNode.Value().(*Limit)
				lob.LowestSell = limit.HeadOrder
				tobChanged = true
			} else {
				lob.LowestSell = nil
			}
			tobChanged = true
		} else if o == lob.HighestBuy {
			// update lob.HighestBuy
			lowerBuyNode := node.Prev()
			if lowerBuyNode != nil {
				limit := lowerBuyNode.Value().(*Limit)
				lob.HighestBuy = limit.HeadOrder
			} else {
				lob.HighestBuy = nil
			}
			tobChanged = true
		}
		// last order for this price limit, remove from the book
		tree.Delete(key)
	} else {
		// update the price limit struct
		limit.Size--
		limit.TotalVolume -= o.Quantity
		if o == limit.HeadOrder {
			limit.HeadOrder = o.NextOrder
		}
		if o == limit.TailOrder {
			limit.TailOrder = o.PrevOrder
		}
		// update the orders list
		if o.PrevOrder != nil {
			o.PrevOrder.NextOrder = o.NextOrder
		}
		if o.NextOrder != nil {
			o.NextOrder.PrevOrder = o.PrevOrder
		}
		// check if affects ToB
		if o.IsSell && o.Price == lob.LowestSell.Price {
			tobChanged = true
		} else if !o.IsSell && o.Price == lob.HighestBuy.Price {
			tobChanged = true
		}
	}
	return tobChanged
}

func (lob *LimitOrderBook) getOrderOutput(accepted bool, o *Order) string {
	sym := "A"
	if !accepted {
		sym = "R"
	}
	return fmt.Sprintf("%s, %d, %d", sym, o.UID, o.ID)
}

func (lob *LimitOrderBook) getTobOutput(isSell bool) string {
	sym := "B"
	price := "-"
	volume := "-"
	var limit *Limit
	if isSell {
		sym = "S"
		if lob.LowestSell != nil {
			limit = lob.LowestSell.ParentLimit
		}
	} else if lob.HighestBuy != nil {
		limit = lob.HighestBuy.ParentLimit
	}
	if limit != nil {
		price = fmt.Sprintf("%d", limit.Price)
		volume = fmt.Sprintf("%d", limit.TotalVolume)
	}
	return fmt.Sprintf("B, %s, %s, %s", sym, price, volume)
}

func (lob *LimitOrderBook) getOrderTree(o *Order) *avl.Tree {
	if o.IsSell {
		return lob.SellTree
	}
	return lob.BuyTree
}

type treeKey struct {
	val int
}

func (k treeKey) String() string {
	return strconv.Itoa(k.val)
}

func (k treeKey) Compare(k2 interface{}) int {
	v2 := k2.(treeKey).val
	if k.val == v2 {
		return 0
	} else if k.val > v2 {
		return 1
	} else {
		return -1
	}
}
