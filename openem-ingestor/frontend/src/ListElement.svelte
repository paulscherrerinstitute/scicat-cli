<script>
  export let id;
  export let value;
  export let status;
  export let progress;
  export let cancelTask;
  export let scheduleTask;
  export let removeTask;
</script>

<div class="listitem">
  <div class="path">
    {value}
  </div>
  <div>
    {status}
  </div>
  <div>
    {progress} %
  </div>
  <div class="buttongrid">
    <button
      class="btn"
      on:click={() => {
        scheduleTask(id);
      }}>Start Ingestion</button
    >
    <button
      class="btn"
      on:click={() => {
        cancelTask(id);
      }}>Cancel</button
    >
    <button
      class="btn"
      on:click={() => {
        removeTask(id);
      }}>Remove</button
    >
  </div>
</div>

<style>
  .listitem {
    display: grid;
    grid-template-columns: 5fr 2fr 2fr 2fr;
    grid-template-rows: auto;
    padding-right: 30px;
  }
  .buttongrid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
  }
  .path {
    text-align: left;
  }
</style>
