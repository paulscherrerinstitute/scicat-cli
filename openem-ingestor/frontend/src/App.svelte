<script lang="ts">
  import logo from "./assets/images/logo-wide-1024x317.png";
  import { SelectFolder } from "../wailsjs/go/main/App.js";
  import { EventsOn } from "../wailsjs/runtime/runtime";
  import List from "./List.svelte";
  import ListElement from "./ListElement.svelte";

  function selectFolder(): void {
    SelectFolder();
  }

  let items = {};

  function newItem(id: string, folder: string): string {
    items[id] = {
      id: id,
      value: folder,
      status: "Selected",
      progress: 0,
      component: ListElement,
    };
    return id;
  }

  EventsOn("folder-added", (id, folder) => {
    newItem(id, folder);
  });
</script>

<main>
  <img alt="OpenEM logo" id="logo" src={logo} />
  <button class="btn" on:click={selectFolder}>Select Folder</button>
  <div>
    <div id="upload-list">
      <List {items} />
    </div>
  </div>
</main>

<style>
  #logo {
    display: block;
    width: 50%;
    height: 50%;
    margin: auto;
    padding: 10% 0 0;
    background-position: center;
    background-repeat: no-repeat;
    background-size: 100% 100%;
    background-origin: content-box;
  }

  .result {
    height: 20px;
    line-height: 20px;
    margin: 1.5rem auto;
  }

  .input-box .btn {
    width: 60px;
    height: 30px;
    line-height: 30px;
    border-radius: 3px;
    border: none;
    margin: 0 0 0 20px;
    padding: 0 8px;
    cursor: pointer;
  }

  .input-box .btn:hover {
    background-image: linear-gradient(to top, #cfd9df 0%, #e2ebf0 100%);
    color: #333333;
  }

  .input-box .input {
    border: none;
    border-radius: 3px;
    outline: none;
    height: 30px;
    line-height: 30px;
    padding: 0 10px;
    background-color: rgba(240, 240, 240, 1);
    -webkit-font-smoothing: antialiased;
  }

  .input-box .input:hover {
    border: none;
    background-color: rgba(255, 255, 255, 1);
  }

  .input-box .input:focus {
    border: none;
    background-color: rgba(255, 255, 255, 1);
  }
</style>
