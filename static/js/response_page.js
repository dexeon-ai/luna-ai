// simple placeholder for refresh logic
document.addEventListener("DOMContentLoaded",()=>{
  console.log("Luna Response Page loaded");
  // auto-refresh overlays hourly
  setInterval(()=>{
    document.querySelectorAll("img").forEach(img=>{
      if(img.src.includes("_matrix.png")){
        img.src = img.src.split("?")[0] + "?t=" + Date.now();
      }
    });
  },60*60*1000);
});
