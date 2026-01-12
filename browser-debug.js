// ============================================
// BROWSER CONSOLE DEBUG SCRIPT
// Run this in browser console (F12) when on chat.html
// ============================================

console.log("🔍 Running Chat Debug Script...");

// Check 1: Is room data in localStorage?
const roomData = localStorage.getItem('currentRoom');
if (roomData) {
  console.log("✅ Room data found in localStorage:");
  console.log(JSON.parse(roomData));
} else {
  console.error("❌ NO room data in localStorage!");
  console.log("💡 This means discovery.html didn't save it or it was cleared");
}

// Check 2: Are the state management scripts loaded?
if (window.StateManager) {
  console.log("✅ StateManager loaded");
  console.log("   Current state:", window.StateManager.getState());
} else {
  console.error("❌ StateManager NOT loaded");
}

if (window.NavigationGuard) {
  console.log("✅ NavigationGuard loaded");
} else {
  console.error("❌ NavigationGuard NOT loaded");
}

// Check 3: Is socket connected?
if (window.socketInstance) {
  console.log("✅ Socket instance exists");
  console.log("   Connected:", window.socketInstance.connected);
  console.log("   Socket ID:", window.socketInstance.id);
} else {
  console.error("❌ No socket instance found");
}

// Check 4: Check if room joined flag is set
if (window.__roomJoined) {
  console.log("✅ Room joined flag is set");
} else {
  console.warn("⚠️ Room joined flag NOT set (might still be joining)");
}

// Check 5: Listen for socket events
if (window.socketInstance) {
  console.log("📡 Monitoring socket events...");
  
  window.socketInstance.on('room_joined', (data) => {
    console.log("🎉 EVENT: room_joined", data);
  });
  
  window.socketInstance.on('error', (data) => {
    console.error("❌ EVENT: error", data);
  });
  
  window.socketInstance.on('authenticated', (data) => {
    console.log("✅ EVENT: authenticated", data);
  });
  
  // Try to manually join room if needed
  const testJoinRoom = () => {
    const roomData = localStorage.getItem('currentRoom');
    if (roomData) {
      const parsed = JSON.parse(roomData);
      console.log("🧪 TEST: Manually calling join_room with:", parsed.roomId);
      window.socketInstance.emit('join_room', { roomId: parsed.roomId });
    }
  };
  
  // Make function available
  window.testJoinRoom = testJoinRoom;
  console.log("💡 TIP: Run 'testJoinRoom()' to manually test joining room");
}

console.log("✅ Debug script complete!");
console.log("---");
console.log("📊 SUMMARY:");
console.log("  - Room data:", roomData ? "✅ Found" : "❌ Missing");
console.log("  - StateManager:", window.StateManager ? "✅ Loaded" : "❌ Missing");
console.log("  - Socket:", window.socketInstance ? "✅ Connected" : "❌ Not connected");
console.log("  - Room joined:", window.__roomJoined ? "✅ Yes" : "⚠️ Not yet");
