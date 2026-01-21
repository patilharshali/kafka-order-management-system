import { useState, useEffect } from 'react';
import './App.css'

function App(){
  const [orderId, setOrderId] = useState(null);
  const [statusUpdates, setStatusUpdates] = useState([]);
  const [userId] = useState(`user_${Math.floor(Math.random()*1000)}`); // Random ID for this session
  const placeOrder = async()=>{
    try{
      const response = await fetch('http://localhost:5005/order', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({userId, item:'Modern Desk Lamp'})
      });
      const data = await response.json();
      setOrderId(data.id || 'Pending...');
      setStatusUpdates([{message: 'Order request submitted...', time:new Date().toLocaleTimeString()}]);
    } catch(err) {
      console.error('Order failed:', err)
    }
  };

  // listen for Kafka updates via SSE
  useEffect(()=>{
    const eventSource = new EventSource(`http://localhost:5001/status/${userId}`);
    eventSource.onmessage = (event) =>{
      const data = JSON.parse(event.data);
      setStatusUpdates((prev)=> [...prev, {
        message: data.status,
        time: new Date().toLocaleTimeString()
      }]);
    };
    return () => eventSource.close();
  }, [userId])

  return (
    <div className ="App">
      <h1>Kafka OMS Application</h1>
      <p> Connected as : <strong> {userId}</strong></p>
      <button onClick = {placeOrder} style = {{padding: '10px 20px', fontSize: '16px'}}>Place Order</button>
      <div className = "timeline">
        {statusUpdates.map((update, i) => (
          <p key={i}><strong>[{update.time}]</strong>{update.message}</p>

        ))}
      </div>
    </div>
  )
}

export default App;